(ns csp-clj.channels.waiters
  "Internal concurrency primitives for channels.
   
   Provides the Waiter and Commit abstractions used to build
   atomic multi-channel operations (select!) and standard blocking
   channel operations on virtual threads."
  (:import
   [java.util.concurrent.locks LockSupport]))

(set! *warn-on-reflection* true)

;; TWO-PHASE COMMIT FOUNDATION
;;
;; This namespace provides the building blocks for blocking channel operations
;; on virtual threads. It implements the two-phase commit pattern used by
;; BufferedChannel and UnbufferedChannel.
;;
;; EXAMPLE: Standard put!/take! operation
;;
;;   ;; Thread A wants to put a value
;;   (let [commit (new-commit)
;;         waiter (->PutWaiter commit :value)]
;;     ;; Phase 1: Under channel lock, enqueue if no match
;;     (.lock channel-lock)
;;     (try
;;       (if-let [taker (poll! takes)]
;;         ;; Found match: complete immediately via try-match!
;;         (try-match! taker waiter :value)
;;         ;; No match: enqueue and release lock
;;         (.add puts waiter))
;;       (finally
;;         (.unlock channel-lock)))
;;
;;     ;; Phase 2: Park thread, wait for completion
;;     (park-and-wait commit nil))  ; blocks here
;;
;;   ;; Thread B wants to take
;;   (let [commit (new-commit)
;;         waiter (->TakeWaiter commit)]
;;     ;; Finds Thread A's PutWaiter in puts queue
;;     ;; try-match! completes both commits, unparks Thread A
;;     )
;;
;; SENTINEL VALUES
;;
;; ^Object EOF - Returned to takes when channel closed (nil to caller)
;; ^Object PUT_FAIL - Returned to puts when channel closed (false to caller)
;; nil Commit state - Means "pending" (operation not yet complete)
;;
;; THREAD SAFETY MODEL
;;
;; Each Commit IS a monitor. Locking is on the Commit object itself:
;;   (locking commit ...)
;;
;; This allows concurrent operations on different commits without contention.
;; A thread only blocks others when accessing the same Commit.
;;
;; DEADLOCK PREVENTION (try-match!)
;;
;; When matching two waiters, locks are acquired in thread-ID order:
;;   [first second] (if (< id1 id2) [c1 c2] [c2 c1])
;;   (locking first
;;     (locking second ...))
;;
;; This prevents A-B/B-A deadlock when two threads try to match simultaneously.
;;
;; VIRTUAL THREADS (JDK 19+)
;;
;; Uses LockSupport/park and unpark which are virtual-thread-aware:
;; - Parking a virtual thread releases its carrier thread
;; - Unpark schedules the virtual thread on any available carrier
;; - NEVER use Thread/sleep or Object/wait (carrier-blocking)
;;
;; See LockSupport javadoc for details on virtual thread scheduling.
;;
;; See also: csp-clj.channels.buffered, csp-clj.channels.unbuffered

;; Sentinel object indicating end-of-file (channel closed, take returns nil)
(def ^Object EOF (Object.))

;; Sentinel object indicating put failed (channel closed, put returns false)
(def ^Object PUT_FAIL (Object.))

;; Commit protocol - each Commit is its own monitor for thread-safe state transitions
(defprotocol ICommitState
  (get-state [commit])
  (set-state! [commit val]))

(deftype Commit [id ^Thread thread ^:volatile-mutable state]
  ICommitState
  (get-state [_] state)
  (set-state! [_ val]
    (set! state val)))

(defn new-commit
  "Creates a new Commit object for the current thread.

   The Commit captures:
   - Thread ID: Used for lock ordering in try-match! (deadlock prevention)
   - Thread reference: Used by LockSupport/unpark to wake this thread
   - State: Initially nil (pending), set to result when completed

   Returns:
     Commit instance with nil state, ready for park-and-wait"
  []
  (let [t (Thread/currentThread)]
    ;; Thread ID is used for deterministic lock ordering
    (->Commit (.threadId t) t nil)))

(defprotocol IWaiter
  (get-commit [waiter]
    "Returns the Commit object associated with this waiter.")
  (commit-val [waiter val]
    "Returns the value that should be set in the commit state upon success.")
  (get-value [waiter]
    "Returns the value associated with this waiter. Returns nil for take waiters."))

(defrecord TakeWaiter [^Commit commit]
  IWaiter
  (get-commit [_] commit)
  (commit-val [_ val] val)
  (get-value [_] nil))

(defrecord PutWaiter [^Commit commit value]
  IWaiter
  (get-commit [_] commit)
  (commit-val [_ val] (not (identical? val PUT_FAIL)))
  (get-value [_] value))

(defrecord AltsTakeWaiter [^Commit commit channel]
  IWaiter
  (get-commit [_] commit)
  (commit-val [_ val] [channel :take (if (identical? val EOF) nil val)])
  (get-value [_] nil))

(defrecord AltsPutWaiter [^Commit commit channel value]
  IWaiter
  (get-commit [_] commit)
  (commit-val [_ val] [channel :put (not (identical? val PUT_FAIL))])
  (get-value [_] value))

;; Result of the blocking path of take!. take-outcome! returns the taken value
;; directly on the fast path (values are never nil; put! rejects nil), nil for
;; EOF, and a Blocked instance when the caller must park. The distinct type
;; lets take! tell a parked result apart from an arbitrary user value, which
;; may itself be a vector or a keyword such as :closed or :block.
(defrecord Blocked [^Commit commit waiter])

(defn poll!
  "Extracts and removes the first element from a queue.

   Wrapper around java.util.Queue/poll for type hinting.
   Returns nil if the queue is empty.

   Used to atomically retrieve and remove waiters from takes/puts queues
   while holding the channel lock."
  [^java.util.Queue q]
  (.poll q))

(defn try-commit!
  "Atomically attempt to fulfill a single waiter.
   Acquires the commit monitor lock. Returns true if successful."
  [waiter val]
  ;; Lock the specific Commit (per-Commit monitor pattern)
  (let [^Commit c (get-commit waiter)]
    (locking c
      ;; Only succeed if state is still nil (pending)
      (if (nil? (get-state c))
        (do
          ;; Transform and store the result value
          (set-state! c (commit-val waiter val))
          ;; Wake the parked virtual thread (JDK 19+ aware)
          (LockSupport/unpark (.-thread c))
          true)
        ;; Already completed by another thread or timeout/interrupt
        false))))

(defn commit-first!
  "Polls q until it finds a pending waiter, commits it with val, and returns
   that waiter. Already-completed waiters are discarded. Returns nil if the
   queue is drained without a successful commit.

   The caller must hold the channel lock."
  [^java.util.Queue q val]
  (loop []
    (when-let [w (.poll q)]
      (if (try-commit! w val)
        w
        (recur)))))

(defn commit-all!
  "Commits every waiter currently in q with val (best effort; already-completed
   waiters are discarded), draining the queue. Used by close! to wake all
   blocked takers/putters.

   The caller must hold the channel lock."
  [^java.util.Queue q val]
  (loop []
    (when-let [w (.poll q)]
      (try-commit! w val)
      (recur))))

(defn try-commit-with!
  "Like try-commit! but computes the committed value as (f x). (f x) is only
   evaluated if the commit is still pending, so callers do not need to allocate
   a thunk.

   The caller must hold the channel lock."
  [waiter f x]
  (let [^Commit c (get-commit waiter)]
    (locking c
      (if (nil? (get-state c))
        (do
          ;; Evaluate (f x) under lock, then transform and store.
          ;; Use case: AltsTakeWaiter removing from buffer only on successful commit
          (set-state! c (commit-val waiter (f x)))
          (LockSupport/unpark (.-thread c))
          true)
        false))))

(defn try-match!
  "Atomically attempt to fulfill two waiters simultaneously.
   Acquires both commit locks in ID order to prevent deadlocks.
   Returns true if BOTH commits were successfully fulfilled."
  [w1 w2 val]
  ;; Get both commits for the rendezvous
  (let [^Commit c1 (get-commit w1)
        ^Commit c2 (get-commit w2)]
    ;; Guard against matching a waiter with itself (e.g., select! on same channel)
    (if (= (.-id c1) (.-id c2))
      false
      ;; DEADLOCK PREVENTION: Lock ordering by thread ID
      ;; Always acquire lower ID first, then higher ID
      ;; This ensures global ordering and prevents A-B/B-A deadlock cycles
      (let [[^Commit first-c ^Commit second-c] (if (< (.-id c1) (.-id c2)) [c1 c2] [c2 c1])]
        (locking first-c
          (locking second-c
            ;; Both commits must still be pending (nil state)
            (if (and (nil? (get-state c1)) (nil? (get-state c2)))
              (do
                ;; Atomically fulfill both waiters with the same value
                (set-state! c1 (commit-val w1 val))
                (set-state! c2 (commit-val w2 val))
                ;; Wake both virtual threads
                (LockSupport/unpark (.-thread c1))
                (LockSupport/unpark (.-thread c2))
                true)
              ;; One or both already completed (race condition)
              false)))))))

(defn match-putter!
  "Poll puts until a live putter is matched against the AltsTakeWaiter, and
   return that putter. Already-completed putters are discarded. Returns nil if
   the queue is drained.

   The caller must hold the channel lock."
  [^java.util.Queue puts waiter]
  (loop []
    (when-let [p (.poll puts)]
      (if (try-match! waiter p (get-value p))
        p
        (recur)))))

(defn match-taker!
  "Poll takes until a live taker is matched against the AltsPutWaiter, and
   return that taker. Already-completed takers are discarded. Returns nil if
   the queue is drained.

   The caller must hold the channel lock."
  [^java.util.Queue takes waiter]
  (loop []
    (when-let [t (.poll takes)]
      (if (try-match! t waiter (get-value waiter))
        t
        (recur)))))

(defn park-and-wait
  "Parks the current virtual thread until the commit state is no longer nil.

   Handles timeouts and Thread/interrupts correctly.
   If a timeout occurs or the thread is interrupted, attempts to lock the
   commit and set the state to :timeout or :interrupted respectively.

   Returns the final state of the commit."
  [^Commit commit timeout-ms]
  (if timeout-ms
    ;; TIMEOUT PATH: Block with deadline
    (let [;; Convert milliseconds to nanoseconds for precise timing
          nanos (* timeout-ms 1000000)
          ;; Calculate absolute deadline (monotonic nanoTime)
          deadline (+ (System/nanoTime) nanos)]
      (loop []
        ;; Check if already completed (another thread fulfilled it)
        (let [current-state (get-state commit)]
          (if-not (nil? current-state)
            ;; Operation completed, return the result
            current-state
            ;; Still pending - check timeout
            (if (Thread/interrupted)
              (do
                (.interrupt (Thread/currentThread))
                (locking commit
                  (if (nil? (get-state commit))
                    (do (set-state! commit :interrupted) :interrupted)
                    (get-state commit))))
              (let [remaining (- deadline (System/nanoTime))]
                (if (<= remaining 0)
                  (locking commit
                    (if (nil? (get-state commit))
                      (do (set-state! commit :timeout) :timeout)
                      (get-state commit)))
                  (do
                    (LockSupport/parkNanos remaining)
                    (recur)))))))))

    ;; INDEFINITE BLOCKING PATH: No timeout
    (loop []
      (let [current-state (get-state commit)]
        (if-not (nil? current-state)
          ;; Completed while we weren't looking
          current-state
          (if (Thread/interrupted)
            ;; Handle interrupt without timeout logic
            (do
              (.interrupt (Thread/currentThread)) ;; ALWAYS re-assert the flag
              (locking commit
                (if (nil? (get-state commit))
                  (do (set-state! commit :interrupted) :interrupted)
                  (get-state commit))))
            ;; Park indefinitely until unparked by try-commit!/try-match!
            ;; LockSupport/park is virtual-thread-aware (unlike Thread/sleep)
            (do
              (LockSupport/park)
              ;; Woke up - loop to check state/interrupt
              (recur))))))))
