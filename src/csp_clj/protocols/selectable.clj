(ns csp-clj.protocols.selectable)

(set! *warn-on-reflection* true)

(defprotocol Selectable
  (try-nonblock! [ch op value]
    "Attempt a non-blocking operation on the channel.
     op is :take or :put.
     For :take, value is ignored.
     For :put, value is the object to put.
     Returns:
     - [ch :take val] on successful take
     - [ch :take nil] if channel is closed
     - [ch :put true] on successful put
     - [ch :put false] if channel is closed
     - :csp-clj.channels.waiters/pending if operation would block")
  (wait! [ch waiter]
    "Enqueue an IWaiter into the channel's appropriate queue (puts or takes) without blocking.
     The waiter should be an AltsTakeWaiter or AltsPutWaiter.")
  (cancel-wait! [ch waiter]
    "Removes a waiter from the channel's queues. Returns true if removed, false otherwise."))

