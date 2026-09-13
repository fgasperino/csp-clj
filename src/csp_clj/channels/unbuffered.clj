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
;; TWO-PHASE COMMIT PATTERN (AND FAST PATHS)
;;
;; Blocking operations use the csp-clj.channels.waiters namespace (see BufferedChannel).
;;
;; Fast paths (rendezvous with a waiting partner, closed-under-lock) complete
;; synchronously under the channel lock and return directly WITHOUT allocating
;; a Commit/Waiter. Only the blocking branch (no partner) allocates a Commit +
;; Waiter, enqueues it, releases the lock, and parks in phase 2.
;;
;; DESIGN NOTE — lost invariant: see the equivalent note in BufferedChannel.
;; The same reasoning applies here: the active side holds the channel lock
;; through phase 1, so no third party can interfere with a fast-path op; the
;; PARTNER side of a rendezvous is still fulfilled through the locked
;; try-commit!; memory visibility is provided by ReentrantLock's unlock fence
;; and the volatile AtomicBoolean `closed`. Future features requiring "every
;; in-flight op has a Commit" must special-case these fast paths or be scoped
;; to blocking operations only.
;;
;; Phase 1 (under lock): Check for matching waiter, or enqueue self.
;; Phase 2 (after unlock): Park until matched partner commits the operation.
;;
;; STATE TRANSITIONS
;;
;; put!:
;; - takes queue non-empty: Direct handoff via try-commit! on the taker (rendezvous)
;; - no takers: Enqueue Commit/Waiter in puts, park until taker arrives
;;
;; take!:
;; - puts queue non-empty: Direct handoff via try-commit! on the putter (rendezvous)
;; - no putters: Enqueue Commit/Waiter in takes, park until putter arrives
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

;; PHASE-1 HELPERS
;;
;; The locked phase of every channel operation lives in these top-level
;; functions rather than inline in the record's method bodies. A `try` in a
;; deftype/defrecord method body is compiled into a capturing AFunction that is
;; allocated on every call (even when no exception occurs); the same `try` in a
;; top-level defn is not. These helpers each acquire the channel lock, resolve
;; the fast paths, and release the lock before returning.

(defn- put-outcome!
  "Phase 1 for put!: returns :closed, :rendezvous, or [:block commit waiter]."
  [^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque takes ^ArrayDeque puts value]
  (try
    (.lock lock)
    (cond
      (.get closed) :closed
      (waiters/commit-first! takes value) :rendezvous
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
  [^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque takes ^ArrayDeque puts]
  (try
    (.lock lock)
    (if-let [putter (waiters/commit-first! puts true)]
      (waiters/get-value putter)
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
  [ch ^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque puts]
  (try
    (.lock lock)
    (if-let [putter (waiters/commit-first! puts true)]
      ;; Immediate handoff with waiting putter
      [ch :take (waiters/get-value putter)]
      (if (.get closed)
        [ch :take nil]
        ;; Cannot complete immediately
        :csp-clj.channels.waiters/pending))
    (finally
      (.unlock lock))))

(defn- try-nonblock-put-outcome!
  "Non-blocking put for select!: returns [ch :put true], [ch :put false], or :pending."
  [ch ^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque takes value]
  (when (nil? value)
    (throw (IllegalArgumentException. "Cannot put nil on channel")))
  (try
    (.lock lock)
    (if (.get closed)
      [ch :put false]
      (if (waiters/commit-first! takes value)
        ;; Immediate handoff to waiting taker
        [ch :put true]
        ;; Cannot complete immediately
        :csp-clj.channels.waiters/pending))
    (finally
      (.unlock lock))))

(defn- wait-outcome!
  "Registers an Alts waiter: matches a partner or enqueues the waiter."
  [^ReentrantLock lock ^AtomicBoolean closed ^ArrayDeque takes ^ArrayDeque puts waiter]
  (.lock lock)
  (try
    (if (instance? csp_clj.channels.waiters.AltsTakeWaiter waiter)
      ;; AltsTakeWaiter: try to find a matching putter
      (if (waiters/match-putter! puts waiter)
        true
        (if (.get closed)
          (waiters/try-commit! waiter waiters/EOF)
          (.add takes waiter)))
      ;; AltsPutWaiter: try to find a matching taker
      (if (.get closed)
        (waiters/try-commit! waiter waiters/PUT_FAIL)
        (if (waiters/match-taker! takes waiter)
          true
          (.add puts waiter))))
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

(defrecord UnbufferedChannel [^ReentrantLock lock
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
      (let [outcome (put-outcome! lock closed takes puts value)]
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
            :rendezvous true)))))

  (take! [this]
    ;; Indefinite take: delegate to the timeout arity with no timeout so the
    ;; two arities can never drift apart.
    (channel-protocol/take! this nil))

  (take! [this timeout-ms]
    (let [outcome (take-outcome! lock closed takes puts)]
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
      (try-nonblock-take-outcome! this lock closed puts)
      (try-nonblock-put-outcome! this lock closed takes value)))

  ;; Register waiter for select! operation
  (wait! [_ waiter]
    ;; If the alts commit is already fulfilled (an earlier wait!
    ;; in the select! slow path matched a partner), return immediately
    ;; without acquiring the lock or polling any queue. Otherwise the
    ;; rendezvous loop below would drain the opposing queue via poll!
    ;; while try-match! always returns false (alts commit non-nil),
    ;; orphaning every polled waiter.
    (when (nil? (waiters/get-state (waiters/get-commit waiter)))
      (wait-outcome! lock closed takes puts waiter)))

  ;; Remove waiter from queue (timeout or interrupt handling)
  (cancel-wait! [_ waiter]
    (cancel-outcome! lock takes puts waiter)))

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
