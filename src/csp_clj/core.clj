(ns csp-clj.core
  "Core API for csp-clj - Communicating Sequential Processes for Clojure.
   
   A CSP implementation leveraging JDK 24+ virtual threads for blocking
   operations. Provides channels, select operations, pub/sub, and pipeline
   processing with a simple, explicit API.
   
   Key Concepts for New Developers:
   - Channels: Buffered and unbuffered queues for communication
   - Blocking: Operations block the current virtual thread (cheap in JDK 24+)
   - Select: Choose between multiple channel operations
   - Pub/Sub: Topic-based message distribution
   - Pipeline: Parallel transducer processing
   
   Primary exports: channel, put!, take!, close!, select!, pipeline,
   pub!, sub!, into-chan!
   
   Requires JDK 24 or later for virtual thread support."
  (:require
   [csp-clj.channels :as channels]
   [csp-clj.buffers :as buffers]))

(set! *warn-on-reflection* true)

(def fixed-buffer
  "Creates a fixed-size buffer for use with channels.
   
   Parameters:
     - capacity: positive integer for buffer size
   
   Returns:
     A buffer implementing csp-clj.protocols.buffer/Buffer
   
   Example:
     (fixed-buffer 10)
     => #csp_clj.buffers.fixed.FixedBuffer{...}
   
   See also: csp-clj.buffers.fixed/create"
  buffers/fixed-buffer)

(def channel
  "Creates a new channel.
   
   Without arguments, creates an unbuffered channel with synchronous
   rendezvous semantics. With a capacity argument, creates a buffered
   channel of that size.
   
   Parameters:
     - capacity: positive integer for buffer size (optional)
   
   Returns:
     A channel implementing csp-clj.protocols.channel/Channel
   
   Example:
     (channel)        ; unbuffered
     (channel 10)     ; buffered with capacity 10
   
   See also: csp-clj.channels/create,
             csp-clj.channels.unbuffered/create,
             csp-clj.channels.buffered/create"
  channels/create)

(def put!
  "Puts a value onto a channel.
   
   Blocks the current virtual thread until:
   - Unbuffered: a consumer takes the value
   - Buffered: space is available in the buffer
   
   Parameters:
     - ch: the channel
     - value: the value to put (cannot be nil)
     - timeout-ms: optional timeout in milliseconds
   
   Returns:
     - true: value was successfully transferred
     - false: channel is closed
     - :timeout: timeout elapsed (if timeout-ms provided)
   
   Example:
     (put! ch :value)       ; blocks indefinitely
     (put! ch :value 1000)  ; times out after 1 second
     => true
   
   See also: csp-clj.channels/put!, take!"
  channels/put!)

(def take!
  "Takes a value from a channel.
   
   Blocks the current virtual thread until a value is available.
   Returns nil when channel is closed and empty (EOF).
   
   Parameters:
     - ch: the channel
     - timeout-ms: optional timeout in milliseconds
   
   Returns:
     - value: the taken value
     - nil: channel is closed and empty, or thread interrupted
     - :timeout: timeout elapsed (if timeout-ms provided)
   
   Example:
     (take! ch)        ; blocks until value available
     (take! ch 1000)   ; times out after 1 second
     => :value
   
   See also: csp-clj.channels/take!, put!"
  channels/take!)

(def closed?
  "Returns true if the channel is closed.
   
   Parameters:
     - ch: the channel
   
   Returns:
     true if closed, false otherwise
   
   Example:
     (closed? ch)
     => false
   
   See also: csp-clj.channels/closed?, close!"
  channels/closed?)

(def close!
  "Closes a channel.
   
   Idempotent operation. Pending takes receive nil (EOF).
   Pending puts receive false. No further values can be put.
   
   Parameters:
     - ch: the channel to close
   
   Returns:
     nil
   
   Example:
     (close! ch)
     => nil
   
   See also: csp-clj.channels/close!, closed?"
  channels/close!)

(def select!
  "Completes one of several channel operations.
   
   Takes a vector of operations and completes exactly one:
   - [channel :take] - attempt to take a value
   - [channel :put value] - attempt to put a value
   
   If multiple operations are ready, one is chosen pseudo-randomly
   for fairness.
   
   Parameters:
     - operations: vector of operation vectors
     - opts: optional map with :timeout key (milliseconds)
   
   Returns vector [channel op value]:
     - [ch :take val] on successful take
     - [ch :take nil] if taking from closed channel
     - [ch :put true] on successful put
     - [ch :put false] if putting to closed channel
     - [nil :other :timeout] if timeout elapsed
     - [nil :other :interrupted] if thread interrupted
     - [nil :other :shutdown] if all channels closed
   
   Example:
     (select! [[ch1 :take] [ch2 :put :value]])
     => [ch1 :take :data]
   
   See also: csp-clj.channels/select!"
  channels/select!)

(def into-chan!
  "Puts all items from a collection into a channel.
   
   The putting happens in a background virtual thread.
   Returns the channel immediately (non-blocking).
   
   Parameters:
     - coll: collection of values to put
     - ch: channel (optional, creates buffered channel if omitted)
     - close?: if true (default), closes channel when done
   
   Returns:
     The channel
   
   Example:
     (into-chan! [1 2 3])           ; creates channel, closes when done
     (into-chan! ch [1 2 3] false)  ; uses existing channel, keeps open
   
   See also: csp-clj.channels/into-chan!"
  channels/into-chan!)

(def pub!
  "Creates a publisher for topic-based message distribution.
   
   Reads from source channel and routes values to subscribers
   based on topic-fn. Each topic gets its own internal multiplexer.
   
   Parameters:
     - source-ch: channel to read from
     - topic-fn: function to extract topic from value
     - buf-fn: optional function (topic -> capacity) for buffer sizes
   
   Returns:
     A publisher implementing csp-clj.protocols.publisher/Publisher
   
   Example:
     (def p (pub! ch :type))              ; topic is :type field
     (def p (pub! ch :type #(if % 10 1))) ; custom buffer per topic
   
   See also: csp-clj.channels/pub!, sub!, unsub!"
  channels/pub!)

(def sub!
  "Subscribes a channel to a publisher topic.
   
   When the publisher receives a value matching the topic,
   it will be put onto the subscribed channel.
   
   Parameters:
     - p: the publisher
     - topic: the topic to subscribe to
     - ch: the channel to receive values
     - close?: if true (default), closes ch when publisher closes
   
   Returns:
     The subscribed channel
   
   Example:
     (sub! p :orders order-ch)
     (sub! p :events event-ch false)  ; don't close with publisher
   
   See also: csp-clj.channels/sub!, pub!, unsub!"
  channels/sub!)

(def unsub!
  "Unsubscribes a channel from a publisher topic.
   
   Parameters:
     - p: the publisher
     - topic: the topic to unsubscribe from
     - ch: the channel to remove
   
   Returns:
     nil
   
   Example:
     (unsub! p :orders order-ch)
     => nil
   
   See also: csp-clj.channels/unsub!, sub!"
  channels/unsub!)

(def unsub-all!
  "Unsubscribes all channels from a publisher.
   
   With one argument, unsubscribes from all topics.
   With two arguments, unsubscribes from specific topic only.
   
   Parameters:
     - p: the publisher
     - topic: specific topic (optional)
   
   Returns:
     nil
   
   Example:
     (unsub-all! p)        ; unsubscribe all from all topics
     (unsub-all! p :orders) ; unsubscribe all from :orders topic
     => nil
   
   See also: csp-clj.channels/unsub-all!, sub!"
  channels/unsub-all!)

(def pipeline
  "Processes values through a transducer with parallelism.
   
   Takes values from 'from' channel, applies transducer xf with
   parallelism n, and puts results to 'to' channel. Maintains
   order relative to inputs.
   
   Parameters:
     - n: parallelism level (number of concurrent operations)
     - to: output channel
     - xf: transducer to apply
     - from: input channel
     - opts: optional map with:
       - :close? - close 'to' when 'from' closes (default true)
       - :executor - :cpu (work-stealing) or :io (virtual threads)
       - :ex-handler - function to handle exceptions
   
   Returns:
     nil (launches background processing)
   
   Example:
     (pipeline 4 out-ch (map inc) in-ch)
     (pipeline 8 out-ch (filter odd?) in-ch {:close? false})
   
   See also: csp-clj.channels/pipeline"
  channels/pipeline)
