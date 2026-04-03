(ns csp-clj.channels.multiplexer
  "Multiplexer implementation for broadcasting channel values.
   
   Provides a mechanism to distribute values from a single source
   channel to multiple tap channels concurrently.
   
   Key Concepts for New Developers:
   - Source channel: The single input channel being read
   - Tap channels: Multiple output channels receiving all values
   - Backpressure: Applied via Phaser - mult waits for all taps
   - Concurrency: Each tap dispatch runs in its own virtual thread
   
   Algorithm Overview:
   1. Virtual thread continuously takes from source
   2. For each value, snapshots current taps (TOCTOU-safe)
   3. Each tap gets its own virtual thread via ExecutorService
   4. Phaser synchronizes completion - applies backpressure
   5. Failed/closed taps are automatically removed
   
   Called by: csp-clj.channels/multiplex, csp-clj.core/multiplex"
  (:require
   [csp-clj.protocols.channel :as protocol-channel]
   [csp-clj.protocols.multiplexer :as protocol-multiplexer])
  (:import
   [java.util.concurrent ConcurrentHashMap Executors]))

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
;; 4. Taps removed during dispatch remain in snapshot but fail gracefully
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
;; closed - AtomicBoolean, true when source closed or error occurred
;;
;; See also: csp-clj.protocols.multiplexer, csp-clj.channels.pubsub (topic-based)
(defrecord Multiplexer [source ^ConcurrentHashMap taps ^java.util.concurrent.ExecutorService executor ^java.util.concurrent.atomic.AtomicBoolean closed]
  protocol-multiplexer/Multiplexer

  ;; TOCTOU RACE HANDLING:
  ;; 1. Check closed flag (fast path)
  ;; 2. Add tap to map
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
        ;; Re-check closed flag to prevent race condition where
        ;; dispatch-loop closed and cleared taps right before we put.
        ;; This ensures tap is either fully registered OR closed, never orphaned.
        (when (.get closed)
          (.remove taps ch)
          (when close?
            (protocol-channel/close! ch)))))
    nil)

  ;; Remove a tap channel. Safe to call from any thread.
  (untap! [_ ch]
    (.remove taps ch)
    nil)

  ;; Remove all taps. Safe to call from any thread.
  (untap-all! [_]
    (.clear taps)
    nil))

(defn- dispatch-loop
  "Background loop that routes messages from source to all taps.

   Algorithm: Runs on a dedicated virtual thread. Takes from source
   channel, then dispatches to all registered taps concurrently.
   Each tap gets its own virtual thread via the executor. Uses a
   Phaser for synchronization - applies backpressure by waiting for
   all taps to complete before taking next value.

   Error handling: Removes taps that reject values or throw exceptions.
   Cleans up resources on source close or exception.

   Called by: create (launched in background virtual thread)"
  [^Multiplexer mult]
  (let [source (:source mult)
        ^ConcurrentHashMap taps (:taps mult)
        ^java.util.concurrent.ExecutorService executor (:executor mult)
        ^java.util.concurrent.atomic.AtomicBoolean closed (:closed mult)]
    (try
      (loop []
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
              (.close executor))
            ;; Value received - dispatch to all taps
            ;; TOCTOU-SAFE SNAPSHOT: Create immutable view of current taps
            ;; ConcurrentHashMap iterator is weakly consistent - safe to snapshot
            (let [entries (vec taps)]
              (if (empty? entries)
                ;; No taps registered, just consume and loop
                (recur)
                ;; DISPATCH PHASE: Send to all taps concurrently
                (let [;; Phaser with 1 party (the dispatcher itself)
                      ;; Each tap will register as an additional party
                      phaser (java.util.concurrent.Phaser. 1)]
                  (doseq [[tap-ch _] entries]
                    ;; Register this tap as a party in the phaser
                    (.register phaser)
                    (try
                      ;; Dispatch to tap in its own virtual thread
                      (.execute executor
                                (fn []
                                  (try
                                    ;; Attempt to put value to tap
                                    (let [success (protocol-channel/put! tap-ch val)]
                                      ;; ERROR SCENARIO 1: Tap closed during put
                                      (when-not success
                                        (.remove taps tap-ch)))
                                    ;; ERROR SCENARIO 2: Tap threw exception
                                    (catch Exception _
                                      (.remove taps tap-ch))
                                    ;; ALWAYS deregister, even on failure
                                    (finally
                                      (.arriveAndDeregister phaser)))))
                      ;; ERROR SCENARIO 3: Executor rejected task (fatal)
                      (catch Exception e
                        ;; Must deregister since we registered but failed to execute
                        (.arriveAndDeregister phaser)
                        (throw e))))
                  ;; BACKPRESSURE POINT: Wait for all taps to complete
                  ;; This blocks the dispatch-loop until every tap has finished
                  ;; If any tap blocks (full buffer, unbuffered wait), mult blocks
                  (.arriveAndAwaitAdvance phaser)
                  (recur)))))))
      ;; Exception handler: Clean up and mark closed
      (catch Exception _
        (.set closed true)
        (doseq [[tap-ch close?] taps]
          (when close?
            (protocol-channel/close! tap-ch)))
        (.clear taps)
        (.close executor)))))

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

   THREAD SAFETY

   All operations (tap!, untap!, untap-all!) are thread-safe and may
   be called from any virtual thread. The taps map uses ConcurrentHashMap
   for lock-free concurrent access.

   Parameters:
     - source-ch: the source channel to read from

   Returns:
     A Multiplexer implementing csp-clj.protocols.multiplexer/Multiplexer

   Example:
     (def m (create ch))
     (tap! m tap-ch1 true)   ; tap-ch1 closes when mult closes
     (tap! m tap-ch2 false)  ; tap-ch2 stays open when mult closes

   See also:
     - csp-clj.protocols.multiplexer for protocol definition
     - csp-clj.channels.pubsub for topic-based routing (alternative)
     - csp-clj.core/multiplex, csp-clj.core/tap! for convenience API"
  [source-ch]
  (let [taps (ConcurrentHashMap.)
        executor (Executors/newVirtualThreadPerTaskExecutor)
        m (->Multiplexer source-ch taps executor (java.util.concurrent.atomic.AtomicBoolean. false))]
    ;; Start the dispatch loop on a virtual thread
    (Thread/startVirtualThread
     (fn []
       (dispatch-loop m)))
    m))
