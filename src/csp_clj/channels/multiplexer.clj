(ns csp-clj.channels.multiplexer
  "Multiplexer implementation for broadcasting channel values.
    
   Provides a mechanism to distribute values from a single source
   channel to multiple tap channels concurrently.
    
   Key Concepts for New Developers:
   - Source channel: The single input channel being read
   - Tap channels: Multiple output channels receiving all values
   - Backpressure: Applied via Phaser - mult waits for all taps
   - Concurrency: Each tap dispatch runs in its own virtual thread
   - Snapshot reuse: A dirty flag avoids re-snapshotting the tap set every value
    
   Algorithm Overview:
   1. Virtual thread continuously takes from source
   2. For each value, uses cached tap snapshot (re-snapshots only when dirty)
   3. Single-tap fast path: dispatches directly on the dispatcher thread
   4. Multi-tap path: each tap gets its own virtual thread via ExecutorService
   5. Phaser synchronizes completion - applies backpressure
   6. Failed/closed taps are automatically removed
    
   Called by: csp-clj.channels/multiplex, csp-clj.core/multiplex"
  (:require
   [csp-clj.protocols.channel :as protocol-channel]
   [csp-clj.protocols.multiplexer :as protocol-multiplexer])
  (:import
   [java.util.concurrent ConcurrentHashMap Executors]
   [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

;; MULTIPLEXER - BROADCAST CHANNEL MECHANISM
;;
;; A multiplexer (mult) distributes values from a single source channel to
;; multiple tap channels concurrently. This implements the fan-out pattern
;; where every tap receives every value from the source.
;;
;; CONCURRENCY MODEL
;;
;; The mult runs a dedicated virtual thread (dispatch-loop) that:
;; 1. Takes values from source channel (blocks if source blocks)
;; 2. Dispatches each value to all taps concurrently via virtual threads
;; 3. Uses a Phaser to synchronize completion (applies backpressure)
;;
;; BACKPRESSURE VIA PHASER
;;
;; The mult waits for ALL taps to accept each value before taking the next.
;; This is implemented using java.util.concurrent.Phaser:
;;
;;   ;; Phaser initialized with 1 party (the dispatcher itself)
;;   (let [phaser (Phaser. 1)]  ; party count = 1
;;     ;; Register one party per tap
;;     (doseq [[tap-ch _] entries]
;;       (.register phaser)      ; party count += 1
;;       (.execute executor #(try ... (finally (.arriveAndDeregister phaser)))))
;;     ;; Wait for all tap parties to arrive
;;     (.arriveAndAwaitAdvance phaser))  ; blocks until all taps complete
;;
;; If any tap blocks (buffer full or unbuffered with no taker), the entire
;; mult blocks, applying backpressure to the source channel.
;;
;; TOCTOU-SAFE SNAPSHOT PATTERN
;;
;; Before dispatching, the mult snapshots the taps map: (vec taps)
;;
;; This is Time-Of-Check-Time-Of-Use safe because:
;; 1. ConcurrentHashMap provides weakly-consistent iterators
;; 2. Iterator sees state at some point at or since creation
;; 3. Taps added after snapshot won't receive THIS value (correct)
;; 4. Taps removed during dispatch remain in snapshot but will receive
;;    the current value before removal takes effect (TOCTOU tradeoff)
;;
;; Example scenario:
;;   T1: Snapshot = [tap1, tap2]
;;   T2: tap! adds tap3 (not in snapshot, won't receive current value)
;;   T1: Dispatches to tap1, tap2 from snapshot (tap3 missed - correct)
;;
;; ERROR HANDLING
;;
;; Three failure scenarios in dispatch-loop:
;;
;; 1. Tap closed during put!: put! returns false, remove from taps
;;    (catch by return value check)
;;
;; 2. Tap throws exception: wrap in try-catch, remove from taps
;;    (prevents one bad tap from breaking entire mult)
;;
;; 3. Executor rejects task: arriveAndDeregister and rethrow
;;    (fatal - executor shutdown or resource exhaustion)
;;
;; FIELDS
;;
;; source - Source channel to read from (single input)
;; taps - ConcurrentHashMap<Channel, Boolean> (channel -> close-on-shutdown?)
;; executor - VirtualThreadPerTaskExecutor for concurrent dispatch
;; ex-handler - Function for dispatch-loop errors
;; closed - AtomicBoolean, true when source closed or error occurred
;; dirty - AtomicBoolean, set by tap!/untap!/untap-all! and by failed-tap
;;         removal to signal the dispatch-loop to re-snapshot the tap set.
;;         The dispatch-loop CAS-resets it to false when it re-snapshots.
;;
;; See also: csp-clj.protocols.multiplexer, csp-clj.channels.pubsub (topic-based)
(defrecord Multiplexer [source ^ConcurrentHashMap taps ^java.util.concurrent.ExecutorService executor ex-handler ^AtomicBoolean closed ^AtomicBoolean dirty]
  protocol-multiplexer/Multiplexer

  ;; TOCTOU RACE HANDLING:
  ;; 1. Check closed flag (fast path)
  ;; 2. Add tap to map, mark dirty
  ;; 3. Re-check closed flag (window: dispatch-loop closed between 1-2)
  ;; 4. If closed in step 3, remove tap and close it if requested
  (tap! [_ ch close?]
    (if (.get closed)
      ;; Already closed: close the new tap immediately if requested
      (when close?
        (protocol-channel/close! ch))
      (do
        ;; Add tap to map (visible to next dispatch-loop iteration)
        (.put taps ch close?)
        ;; Signal the dispatch-loop to re-snapshot its cached tap list
        (.set dirty true)
        ;; Re-check closed flag to prevent race condition where
        ;; dispatch-loop closed and cleared taps right before we put.
        ;; This ensures tap is either fully registered OR closed, never orphaned.
        ;; Note: this can race with dispatch-loop cleanup (both calling close!
        ;; on the same channel). The Channel protocol requires close! to be
        ;; idempotent to handle this safely.
        (when (.get closed)
          (.remove taps ch)
          (when close?
            (protocol-channel/close! ch)))))
    nil)

  ;; Remove a tap channel. Safe to call from any thread.
  ;; Due to snapshot-based dispatch, the channel may receive one
  ;; more value before removal takes effect (TOCTOU tradeoff).
  (untap! [_ ch]
    (.remove taps ch)
    (.set dirty true)
    nil)

  ;; Remove all taps. Safe to call from any thread.
  (untap-all! [_]
    (.clear taps)
    (.set dirty true)
    nil))

(defn- default-ex-handler
  "Default exception handler for multiplexer dispatch-loop errors.
   
   Delegates to thread's uncaught exception handler.
   
   Parameters:
     - ex: the exception/error that occurred
   
   Called by: create (when no custom :ex-handler provided)"
  [ex]
  (let [t (Thread/currentThread)]
    (-> t .getUncaughtExceptionHandler (.uncaughtException t ex)))
  nil)

(defn- dispatch-loop
  "Background loop that routes messages from source to all taps.

   Algorithm: Runs on a dedicated virtual thread. Takes from source
   channel, then dispatches to all registered taps.

   OPTIMIZATIONS (vs original version):
   1. Snapshot reuse — a cached tap-list vector is reused across values
      when the tap set hasn't changed (tracked by the dirty AtomicBoolean).
      Only re-snapshots when tap!/untap!/untap-all! or a failed-tap removal
      sets dirty. Avoids 100k vector allocations for a stable tap set.
   2. Single-tap fast path — when exactly one tap is registered, dispatches
      directly on the dispatcher thread (blocking put!). Skips the executor
      submission and the Phaser entirely. One tap = no parallelism to exploit,
      so the executor hop is pure overhead.

   Backpressure: The dispatcher waits on a per-value Phaser until all tap
   tasks complete. This is semantically identical to the original behavior.

   Error handling: Removes taps that reject values or throw exceptions.
   Cleans up resources on source close or exception.

   Called by: create (launched in background virtual thread)"
  [^Multiplexer mult]
  (let [source (:source mult)
        ^ConcurrentHashMap taps (:taps mult)
        ^java.util.concurrent.ExecutorService executor (:executor mult)
        ex-handler (:ex-handler mult)
        ^AtomicBoolean closed (:closed mult)
        ^AtomicBoolean dirty (:dirty mult)]
    (try
      (loop [entries nil  ;; cached snapshot, nil means "needs (re)snapshot"
             entries-set? false]  ;; have we snapshotted at least once?
        ;; BLOCKING POINT: Wait for value from source (backpressure source here)
        (let [val (protocol-channel/take! source)]
          (if (nil? val)
            ;; EOF: Source closed - cleanup and exit
            (do
              (.set closed true)
              ;; Close all taps that were registered with close?=true
              (doseq [[tap-ch close?] taps]
                (when close?
                  (protocol-channel/close! tap-ch)))
              (.clear taps)
              (.shutdownNow executor))
            ;; Value received - obtain the current tap snapshot
            (let [;; Re-snapshot only if dirty (or first iteration).
                  ;; CAS-reset dirty to false; if it was already false, reuse entries.
                  need-snapshot (or (not entries-set?)
                                    (.getAndSet dirty false))
                  entries (if need-snapshot (vec taps) entries)]
              (if (empty? entries)
                ;; No taps registered, just consume and loop
                (recur entries true)
                ;; DISPATCH PHASE
                (let [n (count entries)]
                  (if (= n 1)
                    ;; SINGLE-TAP FAST PATH: dispatch directly on this thread.
                    ;; One tap = no parallelism to exploit; the executor
                    ;; submission + barrier are pure overhead.
                    (let [[[tap-ch _]] entries]  ;; destructure first MapEntry
                      (try
                        (let [success (protocol-channel/put! tap-ch val)]
                          (when-not success
                            (.remove taps tap-ch)
                            (.set dirty true)))
                        (catch Throwable _
                          (.remove taps tap-ch)
                          (.set dirty true))))
                    ;; MULTI-TAP PATH: dispatch to all taps concurrently via
                    ;; the executor. Uses a per-value Phaser for backpressure.
                    ;; The Phaser's internal spin-then-park is more efficient
                    ;; than a hand-rolled LockSupport barrier for the short
                    ;; waits typical of buffered tap channels.
                    (let [phaser (java.util.concurrent.Phaser. 1)]
                      (doseq [[tap-ch _] entries]
                        (.register phaser)
                        (try
                          (.execute executor
                                    (fn []
                                      (try
                                        (let [success (protocol-channel/put! tap-ch val)]
                                          (when-not success
                                            (.remove taps tap-ch)
                                            (.set dirty true)))
                                        (catch Throwable _
                                          (.remove taps tap-ch)
                                          (.set dirty true))
                                        (finally
                                          (.arriveAndDeregister phaser)))))
                          (catch Throwable e
                            (.arriveAndDeregister phaser)
                            (throw e))))
                      (.arriveAndAwaitAdvance phaser)))
                  (recur entries true)))))))
      ;; Exception/Error handler: Clean up and mark closed
      ;; Using shutdownNow instead of close to avoid deadlock if
      ;; previously-submitted tap tasks are still blocked on put!
      (catch Throwable t
        (try (ex-handler t) (catch Throwable _))
        (.set closed true)
        (doseq [[tap-ch close?] taps]
          (when close?
            (protocol-channel/close! tap-ch)))
        (.clear taps)
        (protocol-channel/close! source)
        (.shutdownNow executor)))))

(defn create
  "Creates and returns a mult(iplexer) for the given source channel.

   A mult runs a background virtual thread that continually reads from
   the source channel and distributes each value concurrently to all
   registered taps (channels).

   BACKPRESSURE BEHAVIOR

   If the source channel blocks, the mult thread blocks.
   If a tap channel blocks (buffer full or unbuffered with no taker),
   the mult blocks from taking the next value from the source channel
   until all taps have accepted the current value (backpressure).

   This ensures that the slowest tap controls the throughput - useful
   for load balancing and preventing memory overflow.

   LIFECYCLE MANAGEMENT

   If the source channel is closed, the mult thread exits and will
   close all taps that were registered with close? = true.

   ERROR HANDLING

   If the dispatch-loop encounters an error (Exception or Error), the
   :ex-handler is called before resource cleanup. By default, errors
   delegate to the thread's uncaught exception handler (stderr).

   Options:
   - :ex-handler - Function to handle dispatch-loop errors

   THREAD SAFETY

   All operations (tap!, untap!, untap-all!) are thread-safe and may
   be called from any virtual thread. The taps map uses ConcurrentHashMap
   for lock-free concurrent access.

   Parameters:
     - source-ch: the source channel to read from
     - opts: optional map with :ex-handler key

   Returns:
     A Multiplexer implementing csp-clj.protocols.multiplexer/Multiplexer

   Example:
     (def m (create ch))
     (def m (create ch {:ex-handler #(log/error \"mult died!\" %)}))
     (tap! m tap-ch1 true)   ; tap-ch1 closes when mult closes
     (tap! m tap-ch2 false)  ; tap-ch2 stays open when mult closes

   See also:
     - csp-clj.protocols.multiplexer for protocol definition
     - csp-clj.channels.pubsub for topic-based routing (alternative)
     - csp-clj.core/multiplex, csp-clj.core/tap! for convenience API"
  ([source-ch]
   (create source-ch nil))
  ([source-ch {:keys [ex-handler] :or {ex-handler default-ex-handler}}]
   (let [taps (ConcurrentHashMap.)
         executor (Executors/newVirtualThreadPerTaskExecutor)
         m (->Multiplexer source-ch taps executor ex-handler
                          (AtomicBoolean. false)
                          (AtomicBoolean. true))]  ;; dirty=true so first iteration snapshots
      ;; Start the dispatch loop on a virtual thread.
      ;; If the thread fails to start, shut down the already-created
      ;; executor to prevent resource leaks.
     (try
       (Thread/startVirtualThread
        (fn []
          (dispatch-loop m)))
       m
       (catch Throwable t
         (.shutdownNow executor)
         (throw t))))))
