(ns csp-clj.buffers
  "Buffer factory functions for csp-clj channels.
   
   This namespace provides functions to create buffer instances that
   can be passed to channel creation functions.
   
   Key Concepts for New Developers:
   - Buffers provide storage for channel values
   - Fixed buffers block puts when full
   - Buffers are not thread-safe by design (channels handle synchronization)
   
   Currently supported buffer types:
   - FixedBuffer: FIFO queue with fixed capacity
   
   Most users should use csp-clj.core/fixed-buffer instead of this
   namespace directly."
  (:require
   [csp-clj.buffers.fixed :as fixed]))

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
  fixed/create)
