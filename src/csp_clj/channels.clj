(ns csp-clj.channels
  "Main channel API implementation for csp-clj.
   
   This namespace provides the primary functions for working with
   channels, including:
   - Channel creation (buffered and unbuffered)
   - Core operations (put!, take!, close!)
   - Select operations (select!)
   - Utilities (into-chan!)
   - Mult/pub-sub patterns
   - Pipeline processing
   
   Key Concepts for New Developers:
   - create: Factory for buffered and unbuffered channels
   - put!/take!: Blocking operations on virtual threads
   - select!: Choose between multiple pending operations
   - multiplex/pub!: Fan-out patterns
   - pipeline: Parallel transducer processing
   
   Most users should use csp-clj.core instead of this namespace
   directly, as core provides convenient aliases."
  (:require
   [csp-clj.protocols.channel :as channel-protocol]
   [csp-clj.protocols.multiplexer :as multiplexer-protocol]
   [csp-clj.protocols.publisher :as publisher-protocol]
   [csp-clj.protocols.selectable :as selectable-protocol]
   [csp-clj.channels.unbuffered :as unbuffered]
   [csp-clj.channels.buffered :as buffered]
   [csp-clj.channels.multiplexer :as multiplexer]
   [csp-clj.channels.pubsub :as pubsub]
   [csp-clj.channels.pipeline :as pipeline]
   [csp-clj.channels.waiters :as waiters]))

(set! *warn-on-reflection* true)

(defn create
  "Creates a new channel.
   
   Without arguments, creates an unbuffered channel with synchronous
   rendezvous semantics. With a capacity or buffer, creates a buffered
   channel of that size.
   
   Parameters:
     - capacity-or-buffer: positive integer for buffer size, or a
       Buffer instance (optional)
   
   Returns:
     A channel implementing csp-clj.protocols.channel/Channel
   
   Example:
     (create)        ; unbuffered
     (create 10)     ; buffered with capacity 10
     (create (csp-clj.buffers/fixed-buffer 5))
   
   See also: csp-clj.core/channel,
             csp-clj.channels.unbuffered/create,
             csp-clj.channels.buffered/create"
  ([]
   (unbuffered/create))
  ([capacity-or-buffer]
   (buffered/create capacity-or-buffer)))

(defn put!
  "Put a value onto a channel. Blocks until:
   - Unbuffered: a consumer takes the value
   - Buffered: space is available in the buffer
   
   Returns:
   - true: value was successfully transferred
   - false: channel is closed (or thread interrupted)
   - :timeout: timeout was specified and elapsed
   
   Two-arity form blocks indefinitely.
   Three-arity form times out after specified milliseconds.
   
   If the calling thread is interrupted during the operation, the thread's
   interrupt flag will be set and the operation will return false.
   
   Examples:
   (put! ch :value)           ; blocks indefinitely
   (put! ch :value 1000)      ; times out after 1000ms"
  ([ch value]
   (channel-protocol/put! ch value))
  ([ch value timeout-ms]
   (channel-protocol/put! ch value timeout-ms)))

(defn take!
  "Take a value from a channel. Blocks until a value is available.
   
   Returns:
   - value: the taken value
   - nil: channel is closed and empty (EOF), or thread was interrupted
   - :timeout: timeout was specified and elapsed
   
   One-arity form blocks indefinitely.
   Two-arity form times out after specified milliseconds.
   
   If the calling thread is interrupted during the operation, the thread's
   interrupt flag will be set and the operation will return nil.
   
   Examples:
   (take! ch)           ; blocks indefinitely
   (take! ch 1000)      ; times out after 1000ms"
  ([ch]
   (channel-protocol/take! ch))
  ([ch timeout-ms]
   (channel-protocol/take! ch timeout-ms)))

(defn closed?
  "Returns true if the channel is closed.
   
   Examples:
   (closed? ch)  ; true or false"
  [ch]
  (channel-protocol/closed? ch))

(defn close!
  "Closes a channel.
   
   Idempotent operation - multiple calls have no additional effect.
   After closing:
   - Pending takes receive nil (EOF)
   - Pending puts receive false
   - No further values can be put onto the channel
   
   Parameters:
     - ch: the channel to close
   
   Returns:
     nil
   
   Example:
     (close! ch)
     => nil
   
   See also: closed?"
  [ch]
  (channel-protocol/close! ch))

(defn multiplex
  "Creates and returns a mult(iplexer) for the given source channel.
   A mult runs a background virtual thread that continually reads from
   the source channel and distributes each value concurrently to all
   registered taps (channels).

   If the source channel blocks, the mult thread blocks.
   If a tap channel blocks (buffer full or unbuffered with no taker),
   the mult blocks from taking the next value from the source channel
   until all taps have accepted the current value (backpressure).

   If the source channel is closed, the mult thread exits and will
   close all taps that were registered with close? = true."
  [source-ch]
  (multiplexer/create source-ch))

(defn tap!
  "Registers a tap channel on a mult.
   When the source channel of the mult receives a value, it will be
   put! onto the tap channel.
   
   If close? is true (the default), closing the source channel will
   also close the tap channel."
  ([mult ch]
   (tap! mult ch true))
  ([mult ch close?]
   (multiplexer-protocol/tap! mult ch close?)
   ch))

(defn untap!
  "Removes a tap channel from a mult."
  [mult ch]
  (multiplexer-protocol/untap! mult ch))

(defn untap-all!
  "Removes all tap channels from a mult."
  [mult]
  (multiplexer-protocol/untap-all! mult))

(defn pub!
  "Creates and returns a pub(lisher) for the given source channel.
   A pub runs a background virtual thread that continually reads from
   the source channel and distributes each value to all channels
   subscribed to the value's topic.

   The topic is determined by calling topic-fn on the value.
   
   buf-fn is an optional function that takes a topic and returns a
   buffer capacity. If not provided, unbuffered channels are used
   internally."
  ([source-ch topic-fn]
   (pubsub/create source-ch topic-fn nil))
  ([source-ch topic-fn buf-fn]
   (pubsub/create source-ch topic-fn buf-fn)))

(defn sub!
  "Subscribes a channel to a topic of a pub(lisher).
   When the pub's source channel receives a value whose topic matches
   this topic, it will be put! onto the subscribed channel.
   
   If close? is true (the default), closing the pub's source channel will
   also close the subscribed channel."
  ([p topic ch]
   (sub! p topic ch true))
  ([p topic ch close?]
   (publisher-protocol/sub! p topic ch close?)
   ch))

(defn unsub!
  "Unsubscribes a channel from a topic of a pub(lisher)."
  [p topic ch]
  (publisher-protocol/unsub! p topic ch))

(defn unsub-all!
  "Unsubscribes all channels from a pub(lisher), or from a specific topic."
  ([p]
   (publisher-protocol/unsub-all! p))
  ([p topic]
   (publisher-protocol/unsub-all! p topic)))

(defn into-chan!
  "Puts all items from coll into the channel. If no channel is provided,
   creates a buffered channel with capacity (max 1 (count coll)).
   
   The putting happens in a background virtual thread.
   Returns the channel immediately.
   
   If close? is true (the default), the channel will be closed after
   all items are put."
  ([coll]
   (into-chan! (create (max 1 (count coll))) coll true))
  ([ch coll]
   (into-chan! ch coll true))
  ([ch coll close?]
   (Thread/startVirtualThread
    #(try
       (loop [items (seq coll)]
         (when items
           (let [item (first items)]
             ;; put! returns false if channel is closed, so we can short-circuit
             (when (put! ch item)
               (recur (next items))))))
       (finally
         (when close?
           (close! ch)))))
   ch))

(defn pipeline
  "Takes elements from the from channel and supplies them to the to
   channel, subject to the transducer xf, with parallelism n.
   
   Because it is parallel, the transducer will be applied independently
   to each element. Outputs will be returned in order relative to the inputs.
   
   Options:
   - :close? (default true) - Close the to channel when from channel closes
   - :executor (default :cpu) - :cpu (work stealing) or :io (virtual threads)
   - :ex-handler - Function to handle exceptions during transduction"
  ([n to xf from]
   (pipeline/pipeline n to xf from))
  ([n to xf from opts]
   (pipeline/pipeline n to xf from opts)))

(defn select!
  "Complete one of several channel operations.
   
   operations is a vector of:
   - [channel :take] - attempt to take a value
   - [channel :put value] - attempt to put a value
   
   Returns a vector [channel op value]:
   - [ch :take val] on successful take
   - [ch :take nil] if taking from a closed channel
   - [ch :put true] on successful put
   - [ch :put false] if putting to a closed channel
   
   Options:
   - :timeout - maximum milliseconds to wait
   
   Returns [nil :other :timeout] if timeout elapses.
   Returns [nil :other :interrupted] if thread is interrupted.
   Returns [nil :other :shutdown] if ALL channels are closed."
  ([operations]
   (select! operations nil))
  ([operations {:keys [timeout]}]
   (when (empty? operations)
     (throw (IllegalArgumentException. "select! requires at least one operation")))

   (let [n (count operations)
         start-idx (if (> n 1) (java.util.concurrent.ThreadLocalRandom/current) 0)
         start-idx (if (> n 1) (.nextInt ^java.util.concurrent.ThreadLocalRandom start-idx n) 0)]

     ;; Fast path: Try non-blocking operations
     ;; Keep track of closed channels to handle the :shutdown case
     (loop [i 0
            closed-count 0
            last-closed-res nil]
       (if (< i n)
         (let [idx (if (> n 1) (unchecked-remainder-int (unchecked-add-int start-idx i) n) 0)
               op-def (nth operations idx)
               ch (first op-def)
               op (second op-def)
               value (if (= op :put) (nth op-def 2) nil)
               res (selectable-protocol/try-nonblock! ch op value)]
           (cond
             (= res :csp-clj.channels.waiters/pending)
             ;; Still blocked on this one
             (recur (inc i) closed-count last-closed-res)

             (or (and (= op :take) (nil? (nth res 2)))
                 (and (= op :put) (false? (nth res 2))))
             ;; Channel is closed.
             (recur (inc i) (inc closed-count) res)

             :else
             ;; Fast path succeeded (e.g. got a value or put succeeded)
             res))

         ;; If we checked all channels and none were ready
         (if (= closed-count n)
           [nil :other :shutdown]

           (if (> closed-count 0)
             ;; If some are closed, return one of the closed results immediately
             last-closed-res

             ;; Slow path: We must wait
             (let [commit (waiters/new-commit)
                   ;; Create the AltsWaiters and enqueue them
                   waiters (object-array n)]

               ;; Enqueue all waiters in pseudo-random order for fairness
               (loop [i 0]
                 (when (< i n)
                   (let [idx (if (> n 1) (unchecked-remainder-int (unchecked-add-int start-idx i) n) 0)
                         op-def (nth operations idx)
                         ch (first op-def)
                         op (second op-def)
                         waiter (if (= op :take)
                                  (waiters/->AltsTakeWaiter commit ch)
                                  (waiters/->AltsPutWaiter commit ch (nth op-def 2)))]
                     (aset waiters idx waiter)
                     (selectable-protocol/wait! ch waiter)
                     (recur (inc i)))))

               ;; Park and wait
               (let [final-state (waiters/park-and-wait commit timeout)]
                 (loop [i 0]
                   (when (< i n)
                     (let [op-def (nth operations i)
                           ch (first op-def)
                           waiter (aget waiters i)]
                       (selectable-protocol/cancel-wait! ch waiter)
                       (recur (inc i)))))
                 (cond
                   (= final-state :timeout) [nil :other :timeout]
                   (= final-state :interrupted) [nil :other :interrupted]
                   :else final-state))))))))))
