(ns csp-clj.channels.pubsub
  "Publish/Subscribe implementation for topic-based message distribution.
   
   Provides a mechanism to route values from a source channel to
   subscriber channels based on a topic function.
   
   Key Concepts for New Developers:
   - Topic function: Extracts topic from values (e.g., :type field)
   - Per-topic multiplexers: Each unique topic gets its own mult
   - Lazy creation: Internal channels created on first subscription
   - Auto-cleanup: Empty topics are removed automatically
   
   Algorithm Overview:
   1. Virtual thread continuously takes from source
   2. Applies topic-fn to extract topic from each value
   3. Routes value to appropriate topic's multiplexer
   4. Each topic maintains its own internal channel + mult
   5. Subscribers tap into the topic's multiplexer
   
   Called by: csp-clj.channels/pub!, csp-clj.core/pub!"
  (:require
   [csp-clj.protocols.channel :as channel-protocol]
   [csp-clj.protocols.multiplexer :as multiplexer-protocol]
   [csp-clj.protocols.publisher :as publisher-protocol]
   [csp-clj.channels.unbuffered :as unbuffered]
   [csp-clj.channels.buffered :as buffered]
   [csp-clj.channels.multiplexer :as multiplexer])
  (:import
   [java.util.concurrent ConcurrentHashMap]
   [java.util.function BiFunction]))

(set! *warn-on-reflection* true)

;; PUBLISH/SUBSCRIBE - TOPIC-BASED MESSAGE ROUTING
;;
;; Pubsub provides topic-based message distribution, contrasting with
;; multiplexer (broadcast to all). Values are routed to subscribers based
;; on a topic extracted via topic-fn.
;;
;; ARCHITECTURE
;;
;;   Source Channel → Publisher → Topic A → [Mult A] → [Subscribers...]
;;                              → Topic B → [Mult B] → [Subscribers...]
;;                              → Topic C → [Mult C] → [Subscribers...]
;;
;; Each unique topic gets its own:
;; - Internal channel (buffered or unbuffered based on buf-fn)
;; - Multiplexer (for broadcasting to topic subscribers)
;;
;; This design allows different topics to have different buffer sizes
;; and backpressure characteristics.
;;
;; LAZY TOPIC CREATION
;;
;; Topics are created on-demand when first subscriber subscribes:
;;
;;   ;; Before: topic-mults = {}
;;   (sub! pub :orders ch)  ; Creates :orders topic + mult
;;   ;; After:  topic-mults = {:orders -> Multiplexer}
;;
;; AUTO-CLEANUP
;;
;; Topics are automatically removed when last subscriber unsubscribes:
;;
;;   ;; Before: topic-mults = {:orders -> Multiplexer with 1 tap}
;;   (unsub! pub :orders ch)  ; Removes last subscriber
;;   ;; After:  topic-mults = {}  ; :orders removed, internal channel closed
;;
;; This prevents memory leaks from abandoned topics.
;;
;; CONCURRENTHASHMAP COMPUTE PATTERN
;;
;; Thread-safe topic operations use ConcurrentHashMap.compute():
;;
;;   (.compute topic-mults topic
;;     (reify BiFunction
;;       (apply [_ key existing-mult]
;;         ;; existing-mult is nil if topic doesn't exist
;;         ;; Return new mult to put in map, or nil to remove
;;         (let [mult (or existing-mult (create-new-mult))]
;;           (tap! mult ch)
;;           mult))))
;;
;; This provides atomic read-modify-write without explicit locks.
;;
;; RACE CONDITION HANDLING
;;
;; The sub! method handles a race condition window:
;;   1. Check closed flag (fast path)
;;   2. Compute: create topic if needed, add subscriber
;;   3. Re-check closed flag (window: pub closed between 1-2)
;;   4. If closed in step 3, undo subscription and close channel
;;
;; This ensures subscribers are never orphaned.
;;
;; PUBSUB VS MULTIPLEXER COMPARISON
;;
;; Use multiplexer when:
;;   - All consumers need all values (broadcast/fan-out)
;;   - Simple one-to-many distribution
;;   Example: Log replication, event broadcasting
;;
;; Use pubsub when:
;;   - Different consumers care about different topics (routing)
;;   - Values have natural categories/routing keys
;;   Example: Order processing by type, chat rooms by channel
;;
;; FIELDS
;;
;; source - Source channel to read from
;; topic-fn - Function (value -> topic) to extract routing key
;; topic-mults - ConcurrentHashMap<topic, Multiplexer>
;; buf-fn - Optional function (topic -> capacity) for per-topic buffering
;; closed - AtomicBoolean, true when source closed or error
;;
;; See also: csp-clj.channels.multiplexer, csp-clj.protocols.publisher
(defrecord Publisher [source topic-fn ^ConcurrentHashMap topic-mults buf-fn ex-handler ^java.util.concurrent.atomic.AtomicBoolean closed]
  publisher-protocol/Publisher

  ;; Subscribe a channel to a topic with optional auto-close on publisher shutdown.
  ;;
  ;; LAZY TOPIC CREATION:
  ;; If topic doesn't exist, creates it with:
  ;;   - Internal channel (buffered if buf-fn provided, else unbuffered)
  ;;   - Multiplexer for broadcasting to subscribers
  ;;
  ;; BUFFER FUNCTION:
  ;; When buf-fn is provided, calls (buf-fn topic) to get buffer capacity.
  ;; If buf-fn returns nil or 0, creates unbuffered channel.
  ;; Negative values have undefined behavior.
  ;;
  ;; RACE CONDITION HANDLING:
  ;; 1. Check closed flag (fast path)
  ;; 2. Compute: create topic if needed, add subscriber via BiFunction
  ;; 3. Re-check closed flag (window: pub closed between 1-2)
  ;; 4. If closed in step 3, undo subscription and close channel if requested
  ;;
  ;; Returns the subscribed channel.
  (sub! [_ topic ch close?]
    (when (nil? topic)
      (throw (IllegalArgumentException. "Topic must not be nil")))
    (if (.get closed)
      ;; Publisher already closed: close channel immediately if requested
      (when close?
        (channel-protocol/close! ch))
      (do
        ;; Atomic topic creation/subscription via ConcurrentHashMap.compute
        ;; BiFunction receives [key, existing-value], returns new value for key
        (.compute topic-mults topic
                  (reify BiFunction
                    (apply [_ _ existing-mult]
                      ;; existing-mult is nil if topic does not exist yet.
                      ;; Create the internal channel and multiplexer lazily.
                      (let [internal-ch (when-not existing-mult
                                          (if buf-fn
                                            (buffered/create (buf-fn topic))
                                            (unbuffered/create)))
                            mult (or existing-mult
                                     (multiplexer/create internal-ch))]
                        (try
                          (multiplexer-protocol/tap! mult ch close?)
                          (catch Throwable e
                            ;; If we just created a new topic and tap! fails,
                            ;; close the internal channel to trigger the
                            ;; multiplexer's EOF cleanup path (dispatch loop
                            ;; exits, executor shutdown).  Prevents resource leak.
                            (when internal-ch
                              (channel-protocol/close! internal-ch))
                            (throw e)))
                        mult))))
        ;; RACE WINDOW HANDLING: Check if pub closed during compute
        ;; This prevents orphaned subscriptions
        (when (.get closed)
          (.compute topic-mults topic
                    (reify BiFunction
                      (apply [_ _ mult]
                        (when mult
                          ;; Undo the subscription
                          (multiplexer-protocol/untap! mult ch)
                          ;; Auto-cleanup: remove topic if no subscribers left
                          (if (.isEmpty ^ConcurrentHashMap (:taps mult))
                            (do (channel-protocol/close! (:source mult))
                                nil) ; Return nil to remove from map
                            mult))))) ; Return mult to keep in map
          ;; Close the channel if requested (user wanted auto-close)
          (when close?
            (channel-protocol/close! ch)))))
    ;; Return the channel (convenience for chaining)
    ch)

  ;; Unsubscribe a channel from a topic.
  ;;
  ;; AUTO-CLEANUP:
  ;; If this was the last subscriber to the topic, the topic is automatically
  ;; removed and its internal channel is closed.
  ;;
  ;; Thread-safe: Uses ConcurrentHashMap.compute for atomic operation.
  ;;
  ;; Returns nil (unlike sub! which returns the channel).
  (unsub! [_ topic ch]
    (when (nil? topic)
      (throw (IllegalArgumentException. "Topic must not be nil")))
    (.compute topic-mults topic
              (reify BiFunction
                (apply [_ _ mult]
                  (when mult
                    ;; Remove subscriber from topic's multiplexer
                    (multiplexer-protocol/untap! mult ch)
                    ;; Check if topic has any subscribers left
                    (if (.isEmpty ^ConcurrentHashMap (:taps mult))
                      ;; Last subscriber removed: cleanup topic
                      (do (channel-protocol/close! (:source mult))
                          nil) ; Return nil to remove topic from map
                      mult))))) ; Return mult to keep topic in map
    nil)

  ;; Unsubscribe all channels from all topics (two arities).
  ;;
  ;; Arity 1: Unsub all channels from all topics
  ;; Arity 2: Unsub all channels from a specific topic
  ;;
  ;; Both arities close the internal channels and remove topics from the map.
  (unsub-all! [_]
    ;; Iterate over snapshot of topics (avoids concurrent modification)
    (doseq [topic (vec (.keySet topic-mults))]
      (.compute topic-mults topic
                (reify BiFunction
                  (apply [_ _ mult]
                    (when mult
                      ;; Remove all subscribers from this topic
                      (multiplexer-protocol/untap-all! mult)
                      ;; If a concurrent sub! added a tap during cleanup,
                      ;; keep the topic alive.  Otherwise remove it.
                      (if (.isEmpty ^ConcurrentHashMap (:taps mult))
                        (do (channel-protocol/close! (:source mult))
                            nil)
                        mult))))))
    nil)

  (unsub-all! [_ topic]
    (.compute topic-mults topic
              (reify BiFunction
                (apply [_ _ mult]
                  (when mult
                    ;; Remove all subscribers from this topic
                    (multiplexer-protocol/untap-all! mult)
                    ;; If a concurrent sub! added a tap during cleanup,
                    ;; keep the topic alive.  Otherwise remove it.
                    (if (.isEmpty ^ConcurrentHashMap (:taps mult))
                      (do (channel-protocol/close! (:source mult))
                          nil)
                      mult)))))
    nil))

(defn- default-ex-handler
  "Default exception handler for publisher dispatch-loop errors.
   
   Delegates to thread's uncaught exception handler.
   
   Parameters:
     - ex: the exception/error that occurred
   
   Called by: create (when no custom :ex-handler provided)"
  [ex]
  (let [t (Thread/currentThread)]
    (-> t .getUncaughtExceptionHandler (.uncaughtException t ex)))
  nil)

(defn- dispatch-loop
  "Background loop that routes messages to topic multiplexers.

   Algorithm: Runs on a dedicated virtual thread. Takes from source
   channel, applies topic-fn to determine topic, then routes to the
   appropriate topic's multiplexer. Each topic has its own internal
   channel and multiplexer for distribution.

   Race handling: Uses ConcurrentHashMap compute operations to ensure
   thread-safe topic creation and cleanup.

   Called by: create (launched in background virtual thread)"
  [^Publisher pub]
  (let [source (:source pub)
        topic-fn (:topic-fn pub)
        ^ConcurrentHashMap topic-mults (:topic-mults pub)
        ex-handler (:ex-handler pub)
        ^java.util.concurrent.atomic.AtomicBoolean closed (:closed pub)]
    (try
      (loop []
        ;; BLOCKING: Wait for value from source channel
        (let [val (channel-protocol/take! source)]
          (if (nil? val)
            ;; EOF: Source closed - cleanup all topics and exit
            (do
              (.set closed true)
              ;; Close all topic internal channels to signal their multiplexers
              (doseq [mult (.values topic-mults)]
                (channel-protocol/close! (:source mult))))
            ;; Value received - route to appropriate topic
            (let [topic (topic-fn val)]
              ;; Guard against nil topic: ConcurrentHashMap rejects nil keys.
              ;; Since no subscriber can ever match a nil topic, values with
              ;; nil topic are silently skipped without affecting the publisher.
              (when topic
                (when-let [mult (.get topic-mults topic)]
                  ;; Route value to topic's internal channel
                  ;; The topic's multiplexer will broadcast to subscribers
                  (channel-protocol/put! (:source mult) val)))
              (recur)))))
      ;; EXCEPTION HANDLING FIX: Changed from Exception to Throwable
      ;; This ensures Error types (OOM, StackOverflow) are handled:
      ;; 1. All topic channels are properly closed
      ;; 2. Publisher state is marked as closed
      ;; 3. Consistent with pipeline exception handling
      (catch Throwable t
        (try (ex-handler t) (catch Throwable _))
        (.set closed true)
        ;; Cleanup all topics on any error
        (doseq [mult (.values topic-mults)]
          (channel-protocol/close! (:source mult)))))))

(defn create
  "Creates and returns a pub(lisher) for the given source channel.

   A pub runs a background virtual thread that continually reads from
   the source channel and distributes each value to all channels
   subscribed to the value's topic.

   The topic is determined by calling topic-fn on the value.

   TOPIC LIFECYCLE EXAMPLE

     ;; Create publisher with topic function
     (def p (create ch :type))  ; route by :type field

     ;; Step 1: Subscribe (creates topic lazily)
     (sub! p :orders order-ch)  ; :orders topic created
     (sub! p :orders admin-ch)  ; second subscriber to :orders

     ;; Step 2: Publish (routes to :orders subscribers)
     (put! ch {:type :orders :data \"xyz\"})  ; both chs receive

     ;; Step 3: Unsubscribe (auto-cleanup when last subscriber leaves)
     (unsub! p :orders order-ch)  ; still has admin-ch
     (unsub! p :orders admin-ch)  ; last subscriber, :orders removed

   OPTIONS MAP

   Options:
   - :buf-fn - Function (topic -> capacity) for per-topic buffering.
               nil or omitted means unbuffered channels (default).
   - :ex-handler - Function called on dispatch-loop errors.
                   Default: delegate to thread's uncaught exception handler.

   Edge cases:
   - buf-fn returns nil or 0 → unbuffered channel created
   - buf-fn returns < 0 or non-number → undefined behavior
    - topic-fn returns nil → value skipped (no subscriber can match a nil topic)

   THREAD SAFETY

   All operations (sub!, unsub!, unsub-all!) are thread-safe.
   Multiple threads can subscribe/unsubscribe concurrently.

   COMPARISON TO MULTIPLEXER

   Multiplexer (csp-clj.channels.multiplexer):
   - Broadcast ALL values to ALL taps
   - Use case: Log replication, event broadcasting

   Pubsub (this namespace):
   - Route values BY TOPIC to interested subscribers
   - Use case: Order processing by type, chat rooms by channel

   Parameters:
     - source-ch: the source channel to read from
     - topic-fn: function to extract topic from values
     - opts: optional map with :buf-fn, :ex-handler keys

   Returns:
     A Publisher implementing csp-clj.protocols.publisher/Publisher

   Example:
     (def p (create ch :type))              ; topic is :type field, unbuffered
     (def p (create ch :type {:buf-fn #(if % 10 1)})) ; custom buffer per topic
     (def p (create ch :type {:ex-handler #(log/error \"pub died!\" %)}))
     (sub! p :orders order-ch)

   See also:
     - csp-clj.channels/pub! for high-level API
     - csp-clj.core/pub! for convenience wrapper
     - csp-clj.channels.multiplexer for broadcast alternative"
  ([source-ch topic-fn]
   (create source-ch topic-fn nil))
  ([source-ch topic-fn {:keys [buf-fn ex-handler] :or {ex-handler default-ex-handler}}]
   (let [topic-mults (ConcurrentHashMap.)
         p (->Publisher source-ch topic-fn topic-mults buf-fn ex-handler (java.util.concurrent.atomic.AtomicBoolean. false))]
      ;; Start dispatch loop on virtual thread
     (Thread/startVirtualThread
      (fn []
        (dispatch-loop p)))
     p)))
