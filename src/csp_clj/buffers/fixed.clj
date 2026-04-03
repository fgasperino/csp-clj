(ns csp-clj.buffers.fixed
  "Fixed-size buffer implementation using ArrayDeque.
   
   Provides FIFO queue semantics.
   This buffer is not thread-safe. Thread safety is provided by the
   channel implementation."
  (:require
   [csp-clj.protocols.buffer :as protocol])
  (:import
   [java.util ArrayDeque]))

(set! *warn-on-reflection* true)

(deftype FixedBuffer [^ArrayDeque queue capacity]
  protocol/Buffer

  (full? [_]
    (>= (.size queue) capacity))

  (add! [_ value]
    (.addLast queue value))

  (remove! [_]
    (.pollFirst queue))

  (size [_]
    (.size queue))

  (capacity [_]
    capacity))

(defn create
  "Create a fixed-size buffer with the specified capacity.
   
   Capacity is always at least 1. Items are queued in FIFO order."
  [capacity]
  (->FixedBuffer (ArrayDeque. (max 1 (int capacity))) (max 1 (int capacity))))
