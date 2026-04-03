(ns csp-clj.protocols.multiplexer
  "Protocol for channel multiplexers (mults).
   
   A multiplexer distributes values from a source channel to multiple
   tap channels concurrently. Each value from the source is sent to
   all registered taps.
   
   Key Characteristics:
   - Broadcast semantics: all taps receive all values
   - Concurrent dispatch: each tap gets its own virtual thread
   - Backpressure: blocks until all taps accept each value
   - Lifecycle management: taps can auto-close with source
   
   Use Case: Broadcasting messages to multiple consumers,
   fan-out patterns, event distribution.")

(set! *warn-on-reflection* true)

(defprotocol Multiplexer
  "Protocol for distributing channel values to multiple taps.
   
   A mult reads from a source channel and sends each value to all
   registered tap channels concurrently.
   
   Backpressure is applied: the mult blocks taking from the source
   until all taps have accepted the current value.
   
   When the source closes, taps with close?=true are also closed."

  (tap! [m ch close?])
  (untap! [m ch])
  (untap-all! [m]))
