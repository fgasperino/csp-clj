(ns csp-clj.channels.unbuffered
  "Unbuffered channel implementation.

   Provides synchronous handoff semantics where:
   - put! blocks until a consumer takes the value
   - take! blocks until a producer offers a value

   This matches core.async unbuffered channel behavior."
  (:require
   [csp-clj.protocols.channel :as channel-protocol]
   [csp-clj.protocols.selectable :as selectable-protocol]
   [csp-clj.channels.waiters :as waiters])
  (:import
   [java.util ArrayDeque]
   [java.util.concurrent.locks ReentrantLock]
   [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

;; UnbufferedChannel implements a channel with synchronous rendezvous semantics.
;;
;; Unlike BufferedChannel, there is no buffer. Every put! must wait for a
;; corresponding take! and vice versa. This is the CSP "synchronous handoff"
;; model - the sender and receiver must both be ready at the same time.
;;
;; CONCURRENCY MODEL
;;
;; All mutable state is protected by a single ReentrantLock (monitor pattern).
;; CRITICAL INVARIANT: The lock is NEVER held while parking a virtual thread.
;;
;; TWO-PHASE COMMIT PATTERN
;;
;; Blocking operations use the csp-clj.channels.waiters namespace (see BufferedChannel).
;; Phase 1 (under lock): Check for matching waiter, or enqueue self.
;; Phase 2 (after unlock): Park until matched partner commits the operation.
;;
;; STATE TRANSITIONS
;;
;; put!:
;; - takes queue non-empty: Direct handoff via try-match! (rendezvous)
;; - no takers: Enqueue in puts, park until taker arrives
;;
;; take!:
;; - puts queue non-empty: Direct handoff via try-match! (rendezvous)
;; - no putters: Enqueue in takes, park until putter arrives
;;
;; SELECT INTEGRATION
;;
;; Same pattern as BufferedChannel but with immediate handoff semantics.
;;
;; FIELDS
;;
;; ^ReentrantLock lock - Mutex protecting all mutable state
;; ^ArrayDeque takes - Queue of TakeWaiter/AltsTakeWaiter (blocked takers)
;; ^ArrayDeque puts - Queue of PutWaiter/AltsPutWaiter (blocked putters)
;; ^AtomicBoolean closed - Thread-safe closed flag
;;
;; See also: csp-clj.channels.waiters, csp-clj.channels.buffered
(defrecord UnbufferedChannel [^ReentrantLock lock
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
            ;; Rendezvous: Must find a taker (no buffer to store value)
            (loop []
              (if-let [taker (waiters/poll! takes)]
                (if (waiters/try-match! taker waiter value)
                  true
                  (recur))
                ;; No taker available, must block
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

    ;; Same logic as put! [this value] but with timeout
    (if (.get closed)
      false
      (let [commit (waiters/new-commit)
            waiter (waiters/->PutWaiter commit value)]
        (.lock lock)
        (try
          (if (.get closed)
            (do (waiters/try-commit! waiter waiters/PUT_FAIL) false)
            (loop []
              (if-let [taker (waiters/poll! takes)]
                (if (waiters/try-match! taker waiter value)
                  true
                  (recur))
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
      ;; Phase 1: Acquire lock, check state, maybe enqueue waiter
      (.lock lock)
      (try
        (loop []
          (if-let [putter (waiters/poll! puts)]
            ;; Rendezvous: Found a putter, complete the handoff
            (if (waiters/try-match! waiter putter (waiters/get-value putter))
              true
              (recur))
            ;; No putter available
            (if (.get closed)
              ;; Channel closed: Signal EOF
              (do (waiters/try-commit! waiter waiters/EOF) false)
              ;; Must block: Enqueue in takes queue
              (do
                (.add takes waiter)
                false))))
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
        (loop []
          (if-let [putter (waiters/poll! puts)]
            (if (waiters/try-match! waiter putter (waiters/get-value putter))
              true
              (recur))
            (if (.get closed)
              (do (waiters/try-commit! waiter waiters/EOF) false)
              (do
                (.add takes waiter)
                false))))
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
          (loop []
            (if-let [putter (waiters/poll! puts)]
              ;; Immediate handoff with waiting putter
              (if (waiters/try-commit! putter true)
                [_ :take (waiters/get-value putter)]
                (recur))
              (if (.get closed)
                [_ :take nil]
                ;; Cannot complete immediately
                :csp-clj.channels.waiters/pending)))
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
            (loop []
              (if-let [taker (waiters/poll! takes)]
                ;; Immediate handoff to waiting taker
                (if (waiters/try-commit! taker value)
                  [_ :put true]
                  (recur))
                ;; Cannot complete immediately
                :csp-clj.channels.waiters/pending)))
          (finally
            (.unlock lock))))))

  ;; Register waiter for select! operation
  (wait! [_ waiter]
    (.lock lock)
    (try
      (if (instance? csp_clj.channels.waiters.AltsTakeWaiter waiter)
        ;; AltsTakeWaiter: Try to find a matching putter
        (if (loop []
              (when-let [p (waiters/poll! puts)]
                (if (waiters/try-match! waiter p (waiters/get-value p))
                  p
                  (recur))))
          true
          (if (.get closed)
            (waiters/try-commit! waiter waiters/EOF)
            (.add takes waiter)))
        ;; AltsPutWaiter: Try to find a matching taker
        (if (.get closed)
          (waiters/try-commit! waiter waiters/PUT_FAIL)
          (if (loop []
                (when-let [t (waiters/poll! takes)]
                  (if (waiters/try-match! t waiter (waiters/get-value waiter))
                    t
                    (recur))))
            true
            (.add puts waiter))))
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
  "Creates an unbuffered channel with synchronous rendezvous semantics.

   DESIGN NOTES

   Unlike buffered channels, there is no storage. put! and take! must both
   be ready at the same time for the operation to complete. This is the
   classic CSP synchronous handoff model.

   When a putter and taker meet, the value is transferred immediately
   without copying or buffering. Both operations complete atomically.

   THREADING

   Channel operations block when no matching partner is available.
   Blocking is implemented via virtual thread parking (see csp-clj.channels.waiters).
   The channel is safe for concurrent use from multiple virtual threads.

   Parameters:
     None

   Returns:
     UnbufferedChannel instance implementing Channel and Selectable protocols

   Example:
     (def ch (create))
     ;; Both operations must rendezvous
     (future (put! ch :value))  ; blocks until take!
     (take! ch)                 ; blocks until put!

   See also: csp-clj.channels.waiters for blocking implementation details,
             csp-clj.channels.buffered for buffered channel alternative"
  []
  (->UnbufferedChannel (ReentrantLock.)
                       (ArrayDeque.)
                       (ArrayDeque.)
                       (AtomicBoolean. false)))