(ns csp-clj.protocols.publisher
  "Protocol for topic-based publish/subscribe channels.
   
   A publisher routes values from a source channel to subscriber
   channels based on a topic function. Each topic maintains its
   own independent distribution mechanism.
   
   Key Characteristics:
   - Topic routing: values routed based on topic-fn(value)
   - Per-topic fan-out: each topic has its own set of subscribers
   - Dynamic subscription: subscribers can join/leave at any time
   - Auto-cleanup: topics with no subscribers are cleaned up
   
   Use Case: Event buses, message routing, category-based
   distribution (e.g., by event type, user ID, etc.).")

(set! *warn-on-reflection* true)

(defprotocol Publisher
  "Protocol for topic-based message distribution.
   
   Reads from a source channel and routes values to subscribers
   based on a topic extraction function.
   
   Each unique topic value gets its own internal distribution
   mechanism (multiplexer). Subscribers receive only values
   matching their subscribed topic."

  (sub! [p topic ch close?])
  (unsub! [p topic ch])
  (unsub-all! [p] [p topic]))
