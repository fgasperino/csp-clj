(ns csp-clj.protocols.buffer
  "Buffer protocol for channel storage.
   
   Defines the interface for buffer implementations used by
   buffered channels. Buffers provide FIFO storage with capacity
   limits.
   
   Key Concepts for New Developers:
   - Buffers are not thread-safe by design
   - Channels handle synchronization around buffer operations
   - Currently only FixedBuffer is implemented
   
   Implementations:
   - csp-clj.buffers.fixed/FixedBuffer")

(set! *warn-on-reflection* true)

(defprotocol Buffer
  "Protocol for channel buffer implementations."

  (full? [buf]
    "Returns true if the buffer cannot accept more items.")

  (add! [buf value]
    "Adds an item to the buffer.")

  (remove! [buf]
    "Removes and returns the next item from the buffer.")

  (size [buf]
    "Returns the current number of items.")

  (capacity [buf]
    "Returns the maximum capacity."))
