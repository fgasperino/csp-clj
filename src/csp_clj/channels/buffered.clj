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
;; TWO-PHASE COMMIT PATTERN
;;
;; Blocking operations use the csp-clj.channels.waiters namespace:
;; - Commit: Holds thread reference and mutable state (nil=pending, else=result)
;; - Waiters: Encapsulate operation (take/put) and associated Commit
;; - park-and-wait: Parks thread, waits for another thread to complete the commit
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
;; - takes queue non-empty: Direct handoff via try-match! (rendezvous)
;; - buffer has space: Add to buffer, complete immediately
;; - buffer full: Enqueue in puts, park until space available
;;
;; take!:
;; - buffer non-empty: Remove from buffer, complete immediately
;;   - Then: Pull from puts queue to refill buffer (backpressure relief)
;; - buffer empty+closed: Return EOF
;; - buffer empty: Enqueue in takes, park until value available
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
(defrecord BufferedChannel [^ReentrantLock lock
                            buf
                            ^ArrayDeque takes
                            ^ArrayDeque puts
                            ^AtomicBoolean closed]
  channel-protocol/Channel

  (put! [this value]
    (when (nil? value)
      (throw (IllegalArgumentException. "Cannot put nil on channel")))

    ;; Fast-path closed check outside lock
    (if (.get closed)
      false
      (let [commit (waiters/new-commit)
            waiter (waiters/->PutWaiter commit value)]
        ;; Phase 1: Acquire lock, check state, maybe enqueue waiter
        (.lock lock)
        (try
          ;; Recheck closed inside lock (race condition window)
          (if (.get closed)
            (do (waiters/try-commit! waiter waiters/PUT_FAIL) false)
            ;; Rendezvous optimization: Direct handoff to waiting taker
            (if (loop []
                  (when-let [t (waiters/poll! takes)]
                    (if (waiters/try-match! t waiter value)
                      t
                      (recur))))
              true
              ;; No takers waiting. Check buffer capacity.
              (if-not (buffer-protocol/full? buf)
                (do
                  (buffer-protocol/add! buf value)
                  (waiters/try-commit! waiter true)
                  true)
                ;; Buffer full, must block. Enqueue in puts queue.
                (do
                  (.add puts waiter)
                  false))))
          (finally
            (.unlock lock)))

        ;; Phase 2: Park and wait for completion (lock released)
        (let [^csp_clj.channels.waiters.Commit commit commit
              state (waiters/get-state commit)]
          (if-not (nil? state)
            state
            (let [res (waiters/park-and-wait commit nil)]
              (if (or (= res :timeout) (= res :interrupted))
                (do
                  (selectable-protocol/cancel-wait! this waiter)
                  res)
                res)))))))

  (put! [this value timeout-ms]
    (when (nil? value)
      (throw (IllegalArgumentException. "Cannot put nil on channel")))

    (if (.get closed)
      false
      (let [commit (waiters/new-commit)
            waiter (waiters/->PutWaiter commit value)]
        (.lock lock)
        (try
          (if (.get closed)
            (do (waiters/try-commit! waiter waiters/PUT_FAIL) false)
            (if (loop []
                  (when-let [t (waiters/poll! takes)]
                    (if (waiters/try-match! t waiter value)
                      t
                      (recur))))
              true
              (if-not (buffer-protocol/full? buf)
                (do
                  (buffer-protocol/add! buf value)
                  (waiters/try-commit! waiter true)
                  true)
                (do
                  (.add puts waiter)
                  false))))
          (finally
            (.unlock lock)))

        (let [^csp_clj.channels.waiters.Commit commit commit
              state (waiters/get-state commit)]
          (if-not (nil? state)
            state
            (let [res (waiters/park-and-wait commit timeout-ms)]
              (if (or (= res :timeout) (= res :interrupted))
                (do
                  (selectable-protocol/cancel-wait! this waiter)
                  res)
                res)))))))

  (take! [this]
    (let [commit (waiters/new-commit)
          waiter (waiters/->TakeWaiter commit)]
      ;; Phase 1: Acquire lock, check buffer state
      (.lock lock)
      (try
        (if (> (buffer-protocol/size buf) 0)
          ;; Value available: Remove from buffer and complete
          (let [val (buffer-protocol/remove! buf)]
            (waiters/try-commit! waiter val)
            ;; Backpressure relief: Pull waiting put into now-empty slot
            (loop []
              (when-let [putter (waiters/poll! puts)]
                (if (waiters/try-commit! putter true)
                  (buffer-protocol/add! buf (waiters/get-value putter))
                  (recur))))
            true)
          ;; Buffer empty
          (if (.get closed)
            ;; Channel closed: Signal EOF
            (do (waiters/try-commit! waiter waiters/EOF) false)
            ;; Must block: Enqueue in takes queue
            (do
              (.add takes waiter)
              false)))
        (finally
          (.unlock lock)))

      ;; Phase 2: Park and wait for completion
      (let [^csp_clj.channels.waiters.Commit commit commit
            state (waiters/get-state commit)
            final-state (if-not (nil? state)
                          state
                          (waiters/park-and-wait commit nil))]
        (when (or (= final-state :timeout) (= final-state :interrupted))
          (selectable-protocol/cancel-wait! this waiter))
        (cond
          (identical? final-state waiters/EOF) nil
          :else final-state))))

  (take! [this timeout-ms]
    (let [commit (waiters/new-commit)
          waiter (waiters/->TakeWaiter commit)]
      (.lock lock)
      (try
        (if (> (buffer-protocol/size buf) 0)
          (let [val (buffer-protocol/remove! buf)]
            (waiters/try-commit! waiter val)
            ;; Try to pull a waiting put into the now-empty buffer slot
            (loop []
              (when-let [putter (waiters/poll! puts)]
                (if (waiters/try-commit! putter true)
                  (buffer-protocol/add! buf (waiters/get-value putter))
                  (recur))))
            true)
          (if (.get closed)
            (do (waiters/try-commit! waiter waiters/EOF) false)
            (do
              (.add takes waiter)
              false)))
        (finally
          (.unlock lock)))

      (let [^csp_clj.channels.waiters.Commit commit commit
            state (waiters/get-state commit)
            final-state (if-not (nil? state)
                          state
                          (waiters/park-and-wait commit timeout-ms))]
        (when (or (= final-state :timeout) (= final-state :interrupted))
          (selectable-protocol/cancel-wait! this waiter))
        (cond
          (identical? final-state waiters/EOF) nil
          :else final-state))))

  (close! [_]
    (.lock lock)
    (try
      ;; Idempotent close: Set flag once, clean up waiters
      (when-not (.get closed)
        (.set closed true)

        ;; Complete all blocked takers with EOF
        (loop []
          (when-let [taker (waiters/poll! takes)]
            (waiters/try-commit! taker waiters/EOF)
            (recur)))

        ;; Fail all blocked putters
        (loop []
          (when-let [putter (waiters/poll! puts)]
            (waiters/try-commit! putter waiters/PUT_FAIL)
            (recur))))
      (finally
        (.unlock lock)))
    true)

  (closed? [_]
    (.get closed))

  selectable-protocol/Selectable

  ;; Non-blocking attempt for select! operations
  (try-nonblock! [_ op value]
    (if (= op :take)
      ;; Try to take without blocking
      (do
        (.lock lock)
        (try
          (if (> (buffer-protocol/size buf) 0)
            (let [val (buffer-protocol/remove! buf)]
              ;; Refill buffer from waiting putters
              (loop []
                (when-let [putter (waiters/poll! puts)]
                  (if (waiters/try-commit! putter true)
                    (buffer-protocol/add! buf (waiters/get-value putter))
                    (recur))))
              [_ :take val])
            (if (.get closed)
              [_ :take nil]
              ;; Cannot complete immediately
              :csp-clj.channels.waiters/pending))
          (finally
            (.unlock lock))))
      ;; Try to put without blocking
      (do
        (when (nil? value)
          (throw (IllegalArgumentException. "Cannot put nil on channel")))
        (.lock lock)
        (try
          (if (.get closed)
            [_ :put false]
            (if (loop []
                  (when-let [t (waiters/poll! takes)]
                    ;; Direct handoff to waiting taker (rendezvous)
                    (if (waiters/try-commit! t value)
                      t
                      (recur))))
              [_ :put true]
              (if-not (buffer-protocol/full? buf)
                (do
                  (buffer-protocol/add! buf value)
                  [_ :put true])
                ;; Cannot complete immediately
                :csp-clj.channels.waiters/pending)))
          (finally
            (.unlock lock))))))

  ;; Register waiter for select! operation
  (wait! [_ waiter]
    (.lock lock)
    (try
      (if (instance? csp_clj.channels.waiters.AltsTakeWaiter waiter)
        ;; AltsTakeWaiter: Try to take from buffer
        (if (> (buffer-protocol/size buf) 0)
          ;; Lazy value extraction: Only remove from buffer on successful commit
          (if (waiters/try-commit-fn! waiter #(buffer-protocol/remove! buf))
            ;; Success: Refill buffer from waiting putters
            (loop []
              (when-let [putter (waiters/poll! puts)]
                (if (waiters/try-commit! putter true)
                  (buffer-protocol/add! buf (waiters/get-value putter))
                  (recur))))
            nil)
          (if (.get closed)
            (waiters/try-commit! waiter waiters/EOF)
            (.add takes waiter)))
        ;; AltsPutWaiter: Try to put to buffer or handoff
        (if (.get closed)
          (waiters/try-commit! waiter waiters/PUT_FAIL)
          (if (loop []
                (when-let [t (waiters/poll! takes)]
                  (if (waiters/try-match! t waiter (waiters/get-value waiter))
                    t
                    (recur))))
            true
            (if-not (buffer-protocol/full? buf)
              ;; Buffer has space: Try to commit then add to buffer
              (if (waiters/try-commit! waiter true)
                (buffer-protocol/add! buf (waiters/get-value waiter))
                nil)
              ;; Buffer full: Enqueue in puts
              (.add puts waiter)))))
      (finally
        (.unlock lock))))

  ;; Remove waiter from queue (timeout or interrupt handling)
  (cancel-wait! [_ waiter]
    (.lock lock)
    (try
      (or (.remove takes waiter)
          (.remove puts waiter))
      (finally
        (.unlock lock)))))

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
