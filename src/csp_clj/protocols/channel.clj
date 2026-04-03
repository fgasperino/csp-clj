(ns csp-clj.protocols.channel
  "Core channel protocol defining the fundamental operations for all
   channel implementations in csp-clj.
   
   This protocol abstracts over buffered and unbuffered channels,
   providing a uniform interface for:
   - Synchronous/asynchronous value transfer
   - Channel lifecycle management
   - Timeout support for blocking operations
   
   Thread Safety:
   All methods in this protocol are thread-safe and may be called
   concurrently from multiple virtual threads.")

(set! *warn-on-reflection* true)

(defprotocol Channel
  "Protocol for channel operations."

  (put! [ch value] [ch value timeout-ms])
  (take! [ch] [ch timeout-ms])
  (close! [ch])
  (closed? [ch]))
