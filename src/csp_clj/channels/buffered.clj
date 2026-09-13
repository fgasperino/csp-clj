(ns csp-clj.channels.buffered
  "Buffered channel implementation.
   
   Provides asynchronous queue semantics where:
   - put! blocks only when buffer is full
   - take! blocks only when buffer is empty
   - Items are queued in FIFO order
   
   Capacity is always at least 1."
  (:require
   [csp-clj.channels.waiters :as waiters]
   [csp-clj.protocols.channel :as channel-protocol]
   [csp-clj.protocols.buffer :as buffer-protocol]
   [csp-clj.protocols.selectable :as selectable-protocol]
   [csp-clj.buffers.fixed :as fixed])
  (:import
   [java.util ArrayDeque]
   [java.util.concurrent.locks ReentrantLock]
   [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

;; BufferedChannel implements a channel with FIFO buffer semantics.
;;
;; CONCURRENCY MODEL
;;
;; All mutable state is protected by a single ReentrantLock (monitor pattern).
;; CRITICAL INVARIANT: The lock is NEVER held while parking a virtual thread.
;;
;; TWO-PHASE COMMIT PATTERN (AND FAST PATHS)
;;
;; Blocking operations use the csp-clj.channels.waiters namespace:
;; - Commit: Holds thread reference and mutable state (nil=pending, else=result)
;; - Waiters: Encapsulate operation (take/put) and associated Commit
;; - park-and-wait: Parks thread, waits for another thread to complete the commit
;;
;; Fast paths (buffer hit, closed-under-lock, rendezvous with a waiting partner)
;; complete synchronously under the channel lock and return directly WITHOUT
;; allocating a Commit/Waiter. Only the blocking branch (no partner and no
;; buffer space / no putter) allocates a Commit + Waiter, enqueues it, releases
;; the lock, and parks in phase 2.
;;
;; DESIGN NOTE — lost invariant: previously every operation, fast or slow, had
;; its own Commit so the code followed a single uniform shape (phase 1 produces
;; a commit state, phase 2 reads it). Fast paths now bypass that machinery.
;; Concurrency safety is preserved because:
;;   - the active side holds the channel lock through phase 1, so no third party
;;     can observe or interfere with a fast-path operation;
;;   - the PARTNER side of a rendezvous is still fulfilled through the locked
;;     try-commit! (so racing with the partner's timeout/interrupt/another
;;     select! is still handled correctly);
;;   - backpressure relief in take! still uses try-commit! on polled putters;
;;   - memory visibility is provided by ReentrantLock's unlock fence and the
;;     volatile AtomicBoolean `closed`, NOT by the Commit's volatile state.
;; Future features that require "every in-flight operation has a Commit"
;; (cancellation, observation, tracing) must special-case these fast paths or
;; be scoped to blocking operations only.
;;
;; Phase 1 (under lock): Check if operation can complete. If not, create Commit
;; and add Waiter to takes/puts queue. Release lock.
;;
;; Phase 2 (after unlock): Park thread via park-and-wait. Another thread will
;; call try-commit! or try-match! to atomically set state and unpark.
;;
;; STATE TRANSITIONS
;;
;; put!:
;; - takes queue non-empty: Direct handoff via try-commit! on the taker (rendezvous)
;; - buffer has space: Add to buffer, return true directly
;; - buffer full: Enqueue Commit/Waiter in puts, park until space available
;;
;; take!:
;; - buffer non-empty: Remove from buffer, return value directly
;;   - Then: Pull from puts queue to refill buffer (backpressure relief)
;; - buffer empty+closed: Return nil (EOF) directly
;; - buffer empty: Enqueue Commit/Waiter in takes, park until value available
;;
;; SELECT INTEGRATION
;;
;; Implements Selectable protocol for multi-channel operations. See
;; csp-clj.channels.waiters/AltsTakeWaiter and AltsPutWaiter for details.
;;
;; FIELDS
;;
;; ^ReentrantLock lock - Mutex protecting all mutable state
;; buf - Buffer protocol instance (FIFO queue, access only under lock)
;; ^ArrayDeque takes - Queue of TakeWaiter/AltsTakeWaiter (blocked takers)
;; ^ArrayDeque puts - Queue of PutWaiter/AltsPutWaiter (blocked putters)
;; ^AtomicBoolean closed - Thread-safe closed flag (fast-path check outside lock)
;;
;; See also: csp-clj.channels.waiters, csp-clj.channels.unbuffered

;; PHASE-1 HELPERS
;;
;; The locked phase of every channel operation lives in these top-level
;; functions rather than inline in the record's method bodies. A `try` in a
;; deftype/defrecord method body is compiled into a capturing AFunction that is
;; allocated on every call (even when no exception occurs); the same `try` in a
;; top-level defn is not. These helpers each acquire the channel lock, resolve
;; the fast paths, and release the lock before returning.

(defn- put-outcome!
  "Phase 1 for put!: returns :closed, :rendezvous, :added, or [:block commit waiter]."
  [^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque takes ^ArrayDeque puts buf value]
  (try
    (.lock lock)
    (cond
      (.get closed) :closed
      (waiters/commit-first! takes value) :rendezvous
      (not (buffer-protocol/full? buf))
      (do (buffer-protocol/add! buf value) :added)
      :else
      (let [commit (waiters/new-commit)
            waiter (waiters/->PutWaiter commit value)]
        (.add puts waiter)
        [:block commit waiter]))
    (finally
      (.unlock lock))))

(defn- take-outcome!
  "Phase 1 for take!: returns the taken value, nil on EOF, or a
   waiters/Blocked when the caller must park."
  [^ReentrantLock lock ^AtomicBoolean closed buf ^ArrayDeque takes ^ArrayDeque puts]
  (try
    (.lock lock)
    (if (> (buffer-protocol/size buf) 0)
      (let [val (buffer-protocol/remove! buf)]
        (when-let [putter (waiters/commit-first! puts true)]
          (buffer-protocol/add! buf (waiters/get-value putter)))
        val)
      (if (.get closed)
        nil
        (let [commit (waiters/new-commit)
              waiter (waiters/->TakeWaiter commit)]
          (.add takes waiter)
          (waiters/->Blocked commit waiter))))
    (finally
      (.unlock lock))))

(defn- try-nonblock-take-outcome!
  "Non-blocking take for select!: returns [ch :take val], [ch :take nil], or :pending."
  [ch ^ReentrantLock lock ^AtomicBoolean closed buf ^ArrayDeque puts]
  (try
    (.lock lock)
    (if (> (buffer-protocol/size buf) 0)
      (let [val (buffer-protocol/remove! buf)]
        (when-let [putter (waiters/commit-first! puts true)]
          (buffer-protocol/add! buf (waiters/get-value putter)))
        [ch :take val])
      (if (.get closed)
        [ch :take nil]
        :csp-clj.channels.waiters/pending))
    (finally
      (.unlock lock))))

(defn- try-nonblock-put-outcome!
  "Non-blocking put for select!: returns [ch :put true], [ch :put false], or :pending."
  [ch ^ReentrantLock lock ^AtomicBoolean closed buf ^ArrayDeque takes value]
  (when (nil? value)
    (throw (IllegalArgumentException. "Cannot put nil on channel")))
  (try
    (.lock lock)
    (if (.get closed)
      [ch :put false]
      (if (waiters/commit-first! takes value)
        [ch :put true]
        (if-not (buffer-protocol/full? buf)
          (do (buffer-protocol/add! buf value) [ch :put true])
          :csp-clj.channels.waiters/pending)))
    (finally
      (.unlock lock))))

(defn- wait-outcome!
  "Registers an Alts waiter: matches a partner, takes/puts to the buffer, or
   enqueues the waiter."
  [^ReentrantLock lock ^AtomicBoolean closed buf ^ArrayDeque takes ^ArrayDeque puts waiter]
  (.lock lock)
  (try
    (if (instance? csp_clj.channels.waiters.AltsTakeWaiter waiter)
      ;; AltsTakeWaiter: try to take from buffer
      (if (> (buffer-protocol/size buf) 0)
        ;; Lazy value extraction: only remove from buffer on successful commit
        (if (waiters/try-commit-with! waiter buffer-protocol/remove! buf)
          ;; Success: refill buffer from waiting putters
          (when-let [putter (waiters/commit-first! puts true)]
            (buffer-protocol/add! buf (waiters/get-value putter)))
          nil)
        (if (.get closed)
          (waiters/try-commit! waiter waiters/EOF)
          (.add takes waiter)))
      ;; AltsPutWaiter: try to put to buffer or handoff
      (if (.get closed)
        (waiters/try-commit! waiter waiters/PUT_FAIL)
        (if (waiters/match-taker! takes waiter)
          true
          (if-not (buffer-protocol/full? buf)
            ;; Buffer has space: try to commit then add to buffer
            (if (waiters/try-commit! waiter true)
              (buffer-protocol/add! buf (waiters/get-value waiter))
              nil)
            ;; Buffer full: enqueue in puts
            (.add puts waiter)))))
    (finally
      (.unlock lock))))

(defn- close-outcome!
  "Idempotently closes the channel and wakes all blocked takers/putters."
  [^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque takes ^ArrayDeque puts]
  (.lock lock)
  (try
    (when-not (.get closed)
      (.set closed true)
      ;; Complete all blocked takers with EOF
      (waiters/commit-all! takes waiters/EOF)
      ;; Fail all blocked putters
      (waiters/commit-all! puts waiters/PUT_FAIL))
    (finally
      (.unlock lock)))
  nil)

(defn- cancel-outcome!
  "Removes a waiter from the takes/puts queues. Returns true if removed."
  [^ReentrantLock lock ^ArrayDeque takes ^ArrayDeque puts waiter]
  (.lock lock)
  (try
    (or (.remove takes waiter)
        (.remove puts waiter))
    (finally
      (.unlock lock))))

(defrecord BufferedChannel [^ReentrantLock lock
                            buf
                            ^ArrayDeque takes
                            ^ArrayDeque puts
                            ^AtomicBoolean closed]
  channel-protocol/Channel

  (put! [this value]
    ;; Indefinite put: delegate to the timeout arity with no timeout so the
    ;; two arities can never drift apart.
    (channel-protocol/put! this value nil))

  (put! [this value timeout-ms]
    (when (nil? value)
      (throw (IllegalArgumentException. "Cannot put nil on channel")))

    (if (.get closed)
      false
      (let [outcome (put-outcome! lock closed takes puts buf value)]
        (if (vector? outcome)
          (let [[_ commit waiter] outcome
                ^csp_clj.channels.waiters.Commit commit commit
                state (waiters/get-state commit)]
            (if-not (nil? state)
              state
              (let [res (waiters/park-and-wait commit timeout-ms)]
                (if (= res :timeout)
                  (do
                    (selectable-protocol/cancel-wait! this waiter)
                    res)
                  (if (= res :interrupted)
                    (do
                      (selectable-protocol/cancel-wait! this waiter)
                      false)
                    res)))))
          (case outcome
            :closed false
            :rendezvous true
            :added true)))))

  (take! [this]
    ;; Indefinite take: delegate to the timeout arity with no timeout so the
    ;; two arities can never drift apart.
    (channel-protocol/take! this nil))

  (take! [this timeout-ms]
    (let [outcome (take-outcome! lock closed buf takes puts)]
      (if (instance? csp_clj.channels.waiters.Blocked outcome)
        (let [^csp_clj.channels.waiters.Blocked blocked outcome
              ^csp_clj.channels.waiters.Commit commit (:commit blocked)
              waiter (:waiter blocked)
              state (waiters/get-state commit)
              final-state (if-not (nil? state)
                            state
                            (waiters/park-and-wait commit timeout-ms))]
          (when (or (= final-state :timeout) (= final-state :interrupted))
            (selectable-protocol/cancel-wait! this waiter))
          (cond
            (= final-state :interrupted) nil
            (identical? final-state waiters/EOF) nil
            :else final-state))
        outcome)))

  (close! [_]
    (close-outcome! lock closed takes puts))

  (closed? [_]
    (.get closed))

  selectable-protocol/Selectable

  ;; Non-blocking attempt for select! operations
  (try-nonblock! [this op value]
    (if (= op :take)
      (try-nonblock-take-outcome! this lock closed buf puts)
      (try-nonblock-put-outcome! this lock closed buf takes value)))

  ;; Register waiter for select! operation
  (wait! [_ waiter]
    ;; If the alts commit is already fulfilled (an earlier wait!
    ;; in the select! slow path matched a partner), return immediately
    ;; without acquiring the lock or polling any queue. Otherwise the
    ;; rendezvous loop below would drain the opposing queue via poll!
    ;; while try-match! always returns false (alts commit non-nil),
    ;; orphaning every polled waiter.
    (when (nil? (waiters/get-state (waiters/get-commit waiter)))
      (wait-outcome! lock closed buf takes puts waiter)))

  ;; Remove waiter from queue (timeout or interrupt handling)
  (cancel-wait! [_ waiter]
    (cancel-outcome! lock takes puts waiter)))

(defn create
  "Creates a buffered channel with the specified buffer.

   The buffer argument can be either:
   - A number (capacity), which creates a FixedBuffer
   - A Buffer instance implementing csp-clj.protocols.buffer/Buffer

   DESIGN NOTES

   Capacity is always >= 1. A buffered channel with capacity 1 behaves
   similarly to an unbuffered channel for single operations, but allows
   pipelining (producer can put next value while consumer processes current).

   THREADING

   Channel operations may block when the buffer is full (put!) or empty (take!).
   Blocking is implemented via virtual thread parking (see csp-clj.channels.waiters).
   The channel is safe for concurrent use from multiple virtual threads.

   Parameters:
     - buffer-or-capacity: Buffer instance or positive integer

   Returns:
     BufferedChannel instance implementing Channel and Selectable protocols

   Example:
     (create 10)           ; buffered with capacity 10
     (create (fixed-buffer 5)) ; with explicit buffer

   See also: csp-clj.channels.waiters for blocking implementation details"
  ([]
   (create 1))
  ([buffer-or-capacity]
   (let [buf (if (satisfies? buffer-protocol/Buffer buffer-or-capacity)
               buffer-or-capacity
               (fixed/create buffer-or-capacity))]
     (->BufferedChannel (ReentrantLock.)
                        buf
                        (ArrayDeque.)
                        (ArrayDeque.)
                        (AtomicBoolean. false)))))
