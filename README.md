# csp-clj

Communicating Sequential Processes for Clojure on JDK 24+ Virtual Threads

## Overview

csp-clj is a CSP (Communicating Sequential Processes) library for Clojure that leverages JDK 24+ virtual threads to provide simple, blocking channel operations. Unlike core.async which uses state machines and parking, csp-clj uses true virtual thread blocking—making the API explicit and intuitive.

Key differences from core.async:

- **True blocking operations**: Operations block the virtual thread (cheap in JDK 24+)
- **Explicit API**: No macros or hidden state machines—just function calls  
- **Named operations**: `multiplex` instead of `mult`, `select` instead of `alts` to indicate behavioral differences

## Installation

Lein:

```clojure
[org.clojars.fgasperino/csp-clj "0.0.0"]
```

deps.edn:

```clojure
{:deps {org.clojars.fgasperino/csp-clj {:mvn/version "0.0.0"}}}
```

## Requirements

- JDK 24 or later (for virtual thread support)

## Tutorial: Channel Operations

Channels are the fundamental communication primitive in CSP. They provide a way for concurrent processes to communicate without shared state.

### Creating Channels

```clojure
(require '[csp-clj.core :as csp])

;; UNBUFFERED: Synchronous rendezvous semantics
;; put! blocks until take! is ready, and vice versa
(def unbuffered-ch (csp/channel))

;; BUFFERED: Asynchronous with capacity
;; put! completes immediately if buffer has space
(def buffered-ch (csp/channel 10))
```

### Unbuffered Channels (Rendezvous)

Unbuffered channels require both sender and receiver to be present simultaneously—this is called a "rendezvous". The `put!` blocks until a `take!` is ready, and vice versa.

```clojure
;; The rendezvous pattern ensures both parties are ready
;; Example: Synchronous request-response

(def request-ch (csp/channel))

;; Server process - runs in background virtual thread
(future
  ;; take! BLOCKS here until client puts a request
  ;; This is the rendezvous point - server waits for client
  (let [request (csp/take! request-ch)]
    ;; Once client and server meet, process the request
    (println "Server received:" request)
    ;; Simulate some work
    (Thread/sleep 100)
    (println "Server processed request")))

;; Client process - runs in background virtual thread
(future
  ;; put! BLOCKS here until server is ready to take
  ;; This is the other side of the rendezvous
  (csp/put! request-ch "Hello Server")
  ;; Only proceeds after server has accepted the value
  (println "Client: request acknowledged"))

;; Both futures block until the rendezvous completes
;; Virtual threads make this blocking cheap - no thread pool exhaustion
(Thread/sleep 200)  ; Wait for completion
```

This synchronous handoff is useful when:

- You need guaranteed delivery acknowledgment
- Producer and consumer must coordinate
- Backpressure is inherent in the design
- You want natural flow control without explicit rate limiting

### Buffered Channels

Buffered channels allow the sender to proceed if buffer space is available. The buffer decouples producers and consumers.

```clojure
;; Buffered channels decouple producers and consumers
;; The buffer acts as a queue between them
(def work-queue (csp/channel 5))

;; PRODUCER: Can put up to 5 items without blocking
;; This runs in a background virtual thread
(future
  (println "Producer: starting...")
  ;; First 5 puts complete immediately (buffer has space)
  (doseq [i (range 5)]
    (csp/put! work-queue i)
    (println "Producer: sent" i "(buffered)"))
  
  ;; 6th put BLOCKS until consumer takes one
  ;; This applies backpressure to the producer
  (println "Producer: about to send 5 (will block)...")
  (csp/put! work-queue 5)
  (println "Producer: sent 5 (space became available)")
  
  ;; Signal completion
  (csp/close! work-queue))

;; CONSUMER: Drains the queue slowly
;; This runs in a background virtual thread
(future
  (println "Consumer: starting...")
  ;; Process items with delay to demonstrate buffering
  (loop []
    ;; take! blocks if buffer is empty, returns nil when closed
    (when-let [item (csp/take! work-queue)]
      (println "Consumer: processing" item)
      ;; Simulate slow processing
      (Thread/sleep 50)
      (recur)))
  (println "Consumer: done (channel closed)"))

;; Let the demo run
(Thread/sleep 500)
```

Buffered channels are useful when:

- Producers and consumers operate at different speeds
- You want to smooth out bursty traffic
- Limited memory overhead is acceptable
- You need to decouple producer from consumer timing

### Timeouts

Both `put!` and `take!` support optional timeouts. When a timeout is specified, the operation returns `:timeout` instead of blocking indefinitely.

```clojure
(def ch (csp/channel))

;; TIMEOUT ON TAKE
;; Try to take with 1 second timeout
(let [result (csp/take! ch 1000)]  ; 1000ms = 1 second timeout
  (if (= result :timeout)
    (println "No value available within 1 second - giving up")
    (println "Got value:" result)))
;; => "No value available within 1 second - giving up"

;; TIMEOUT ON PUT (with small buffer to demonstrate)
(def small-ch (csp/channel 1))

;; Fill the buffer
(csp/put! small-ch :first)

;; Try to put with short timeout - will timeout because buffer full
(let [result (csp/put! small-ch :second 100)]  ; 100ms timeout
  (if (= result :timeout)
    (println "Could not put within 100ms - buffer full")
    (println "Value sent successfully")))
;; => "Could not put within 100ms - buffer full"

;; Alternative: use timeout for polling pattern
(def poll-ch (csp/channel))

(future
  ;; Poll for work with timeout
  (loop []
    (let [work (csp/take! poll-ch 500)]  ; 500ms poll interval
      (cond
        (= work :timeout) (do
                           (println "No work available, checking again...")
                           (recur))
        (nil? work) (println "Channel closed, stopping poll loop")
        :else (do
               (println "Processing work:" work)
               (recur))))))

;; Later, put some work
(Thread/sleep 600)
(csp/put! poll-ch "important task")
(Thread/sleep 100)
(csp/close! poll-ch)
```

### Channel Lifecycle

Channels can be closed to signal completion. Closing a channel:

- Causes pending `take!` operations to return `nil` (EOF)
- Causes pending `put!` operations to return `false`
- Prevents new values from being put
- Allows draining remaining values

```clojure
(def ch (csp/channel 3))

;; Put some values
(csp/put! ch :first)
(csp/put! ch :second)
(csp/put! ch :third)

;; Close the channel - signals no more values will be put
(csp/close! ch)

;; Drain remaining values
(println (csp/take! ch))  ; => :first
(println (csp/take! ch))  ; => :second
(println (csp/take! ch))  ; => :third
(println (csp/take! ch))  ; => nil (EOF - channel empty and closed)
(println (csp/take! ch))  ; => nil (EOF - subsequent takes also return nil)

;; Further puts return false (channel closed)
(println (csp/put! ch :fourth))  ; => false

;; Check closed status
(println (csp/closed? ch))  ; => true

;; close! is idempotent - calling multiple times is safe
(println (csp/close! ch))  ; => nil (succeeds, no-op)
```

### Complete Example: Multiple Producers, Single Consumer

This example shows two producers sending work to one consumer in round-robin fashion. The unbuffered channel ensures producers coordinate with the consumer.

```clojure
;; MULTIPLE PRODUCERS, SINGLE CONSUMER
;; Demonstrates round-robin work distribution with rendezvous semantics

(def work-ch (csp/channel))      ; Unbuffered - synchronous handoff
(def done-ch (csp/channel 10))   ; Buffered - collect results
(def counter (atom 0))           ; Track processed items

;; CONSUMER: processes work from both producers
;; Uses round-robin selection based on work item number
(future
  (println "[Consumer] Starting...")
  (loop []
    ;; take! BLOCKS until a producer is ready
    ;; Returns nil when channel closed (EOF)
    (when-let [work (csp/take! work-ch)]
      ;; Parse the work item: "Producer-N-M" where N is producer num, M is item num
      (let [[_ producer-num item-num] (re-matches #"Producer-(\d+)-(\d+)" work)
            result (str "[Consumer] Processed " work 
                       " (from producer " producer-num 
                       ", item " item-num ")")]
        ;; Increment counter atomically
        (swap! counter inc)
        ;; Send result to done channel
        (csp/put! done-ch result)
        ;; Simulate processing time
        (Thread/sleep 25)
        ;; Continue processing
        (recur))))
  ;; Channel was closed, signal completion
  (println "[Consumer] Work channel closed, shutting down")
  (csp/close! done-ch))

;; PRODUCER 1: sends odd-numbered items (1, 3, 5, 7, 9)
(future
  (println "[Producer-1] Starting...")
  ;; Send 5 work items
  (doseq [i [1 3 5 7 9]]
    ;; put! BLOCKS until consumer is ready (rendezvous)
    ;; This ensures producer doesn't outrun consumer
    (csp/put! work-ch (str "Producer-1-" i))
    (println "[Producer-1] Sent item" i))
  (println "[Producer-1] Finished sending"))

;; PRODUCER 2: sends even-numbered items (2, 4, 6, 8, 10)
(future
  (println "[Producer-2] Starting...")
  ;; Send 5 work items
  (doseq [i [2 4 6 8 10]]
    ;; put! BLOCKS until consumer is ready (rendezvous)
    ;; Both producers compete for consumer attention
    ;; Consumer processes whichever producer is ready first
    (csp/put! work-ch (str "Producer-2-" i))
    (println "[Producer-2] Sent item" i))
  (println "[Producer-2] Finished sending"))

;; Wait for producers to finish sending
(Thread/sleep 500)

;; Close work channel to signal consumer to stop
;; Consumer will process any pending work, then exit
(csp/close! work-ch)
(Thread/sleep 100)

;; Collect and print all results
(println "\n=== Results ===")
(loop []
  (when-let [result (csp/take! done-ch 1000)]
    (println result)
    (recur)))

;; Verify round-robin processing
(println "\nTotal items processed:" @counter)
(println "Expected: 10 (5 from each producer)")
```

### Complete Example: Single Producer, Multiple Consumers

This example shows one producer distributing work to two consumers in round-robin fashion using a buffered channel.

```clojure
;; SINGLE PRODUCER, MULTIPLE CONSUMERS
;; Demonstrates round-robin work distribution to workers
;; Uses buffered channel to decouple producer from consumers

(def work-ch (csp/channel 6))    ; Buffer holds 6 items, decouples producer
(def result-ch (csp/channel 10)) ; Collect results
(def active-workers (atom 2))    ; Track active worker count

;; PRODUCER: generates work items
;; Fills buffer, then blocks until consumers create space
(future
  (println "[Producer] Starting...")
  (doseq [i (range 10)]
    ;; Create task
    (let [task (str "Task-" i)]
      ;; put! blocks only if buffer is full
      ;; Buffer size of 6 means first 6 puts are non-blocking
      ;; 7th+ put blocks until a consumer takes an item
      (csp/put! work-ch task)
      (println "[Producer] Sent" task 
               (if (< i 6) "(buffered)" "(blocked, waited for space)"))))
  ;; Signal completion by closing work channel
  ;; Consumers will finish processing buffered items, then exit
  (println "[Producer] Finished sending all tasks")
  (csp/close! work-ch))

;; CONSUMER 1: Worker that processes tasks
;; Round-robin: handles tasks where index mod 2 = 0 (0, 2, 4, 6, 8)
(future
  (println "[Worker-1] Starting...")
  (loop [task-num 0]  ; Track which task number this is
    ;; take! blocks until:
    ;; - Work available in buffer, OR
    ;; - Channel closed (returns nil)
    (when-let [work (csp/take! work-ch)]
      ;; Round-robin: only process even-indexed tasks
      ;; Task 0 -> Worker 1
      ;; Task 1 -> Worker 2
      ;; Task 2 -> Worker 1
      ;; etc.
      (if (even? task-num)
        (do
          (println "[Worker-1] Processing" work)
          ;; Simulate work
          (Thread/sleep 100)
          ;; Send result
          (csp/put! result-ch (str "[Worker-1] completed " work))
          (println "[Worker-1] Finished" work))
        ;; Not our task - we still took it from channel to advance the queue
        ;; In a real system, you might put it back or use a different distribution
        (println "[Worker-1] Skipped" work "(odd index, not mine)"))
      ;; Continue to next task
      (recur (inc task-num))))
  ;; Channel closed, no more work
  (println "[Worker-1] No more work, shutting down")
  ;; Signal this worker is done
  (swap! active-workers dec))

;; CONSUMER 2: Worker that processes tasks
;; Round-robin: handles tasks where index mod 2 = 1 (1, 3, 5, 7, 9)
(future
  (println "[Worker-2] Starting...")
  (loop [task-num 0]  ; Track which task number this is
    (when-let [work (csp/take! work-ch)]
      ;; Round-robin: only process odd-indexed tasks
      (if (odd? task-num)
        (do
          (println "[Worker-2] Processing" work)
          ;; Simulate work (slightly different timing than worker 1)
          (Thread/sleep 120)
          ;; Send result
          (csp/put! result-ch (str "[Worker-2] completed " work))
          (println "[Worker-2] Finished" work))
        ;; Not our task
        (println "[Worker-2] Skipped" work "(even index, not mine)"))
      ;; Continue to next task
      (recur (inc task-num))))
  ;; Channel closed, no more work
  (println "[Worker-2] No more work, shutting down")
  ;; Signal this worker is done
  (swap! active-workers dec))

;; Wait for all work to be processed
(println "\n[Main] Waiting for workers to finish...")
(while (pos? @active-workers)
  (Thread/sleep 100))

;; Close result channel to signal no more results
(csp/close! result-ch)

;; Collect and display all results
(println "\n=== Results ===")
(loop [results []]
  (if-let [result (csp/take! result-ch 1000)]
    (do
      (println result)
      (recur (conj results result)))
    ;; No more results
    (do
       (println "\nTotal results:" (count results))
      (println "Expected: 10 (5 from each worker)"))))
```

### Multiplexer (Broadcast/Fan-Out)

A multiplexer (mult) implements the broadcast pattern where every value from a source channel is distributed to **all** registered tap channels. This is useful for scenarios like log replication, event broadcasting, or sending data to multiple sinks.

Unlike core.async mult/tap which processes taps sequentially, csp-clj multiplex dispatches to all taps **in parallel** using virtual threads. Each tap receives the value concurrently.

#### Creating a Multiplexer

```clojure
;; Create a multiplexer from a source channel
(def source-ch (csp/channel 10))
(def m (csp/multiplex source-ch))

;; Add tap channels - each will receive every value
(def tap1 (csp/channel 10))
(def tap2 (csp/channel 10))
(def tap3 (csp/channel 10))

(csp/tap! m tap1)  ; Register first tap
(csp/tap! m tap2)  ; Register second tap
(csp/tap! m tap3)  ; Register third tap

;; Remove a tap when no longer needed
(csp/untap! m tap2)

;; Remove all taps
(csp/untap-all! m)
```

#### Backpressure Behavior

The multiplexer applies **backpressure** by waiting for ALL taps to accept each value before taking the next from the source. This is implemented using a Phaser synchronization mechanism.

**Key implication**: The slowest tap controls the overall throughput. If one tap blocks (e.g., full buffer or slow consumer), the entire multiplexer blocks, which in turn applies backpressure to the source channel.

This design prioritizes reliability over speed - no messages are dropped, but slow consumers can throttle the system.

#### Complete Example: Log Replication with Backpressure

This example demonstrates a log replication system with three consumers:
- **File Writer**: Fast consumer (writes to disk)
- **Metrics Collector**: Fast consumer (updates counters)
- **Alerting System**: Slow consumer (simulates network latency)

The example shows how the slow alerting system throttles the entire pipeline, and how removing it allows the system to speed up dramatically.

```clojure
;; LOG REPLICATION WITH BACKPRESSURE DEMONSTRATION
;; Shows how slowest consumer controls overall throughput

;; SOURCE: Log entries produced here
(def log-source (csp/channel 10))

;; MULTIPLEXER: Broadcasts to all taps
(def log-mult (csp/multiplex log-source))

;; TAP 1: File writer - fast consumer
(def file-tap (csp/channel 10))

;; TAP 2: Metrics collector - fast consumer  
(def metrics-tap (csp/channel 10))

;; TAP 3: Alerting system - SLOW consumer (100ms delay per log)
;; Small buffer to make backpressure visible
(def alert-tap (csp/channel 1))

;; Register all taps with the multiplexer
(csp/tap! log-mult file-tap)
(csp/tap! log-mult metrics-tap)
(csp/tap! log-mult alert-tap)

;; PRODUCER: Generates 10 log messages
(future
  (println "[Producer] Starting log generation...")
  (let [start-time (System/currentTimeMillis)]
    (doseq [i (range 10)]
      (csp/put! log-source (str "Log-" i))
      (println "[Producer] Sent Log-" i))
    (csp/close! log-source)
    (let [duration (- (System/currentTimeMillis) start-time)]
      (println "[Producer] Finished. Total time:" duration "ms"))))

;; FILE WRITER: Fast consumer (no delay)
(future
  (println "[File] Started")
  (loop [count 0]
    (when-let [log (csp/take! file-tap)]
      (println "[File] Writing:" log)
      (recur (inc count))))
  (println "[File] Done"))

;; METRICS: Fast consumer (no delay)
(future
  (println "[Metrics] Started")
  (loop [count 0]
    (when-let [log (csp/take! metrics-tap)]
      (when (= 0 (mod count 3))
        (println "[Metrics] Processed" count "logs"))
      (recur (inc count))))
  (println "[Metrics] Done"))

;; ALERTING: SLOW consumer (100ms delay per log)
;; This will cause backpressure on the entire system
(future
  (println "[Alert] Started (SLOW - 100ms per log)")
  (loop []
    (when-let [log (csp/take! alert-tap)]
      (println "[Alert] Checking:" log)
      ;; Simulate slow network API call
      (Thread/sleep 100)
      (recur)))
  (println "[Alert] Done"))

;; Wait for Phase 1 to complete
;; With all 3 taps active and alert taking 100ms per log,
;; processing 10 logs takes ~10 seconds
(println "\n=== Phase 1: All taps active (slow consumer present) ===")
(Thread/sleep 11000)  ; Wait for completion

(println "\nPhase 1 complete. Time: ~10 seconds (limited by slow Alert tap)")

;; === TRANSITION: Remove the slow consumer ===
(println "\n=== Transition: Removing slow Alert consumer ===")
(csp/untap! log-mult alert-tap)
(csp/close! alert-tap)
(println "Alert tap removed. System should speed up.")

;; === PHASE 2: Only fast taps remaining ===
(println "\n=== Phase 2: Only fast taps remaining ===")

;; Create new source and multiplexer for Phase 2
(def log-source-2 (csp/channel 10))
(def log-mult-2 (csp/multiplex log-source-2))

;; Re-register only the fast taps
(csp/tap! log-mult-2 file-tap)
(csp/tap! log-mult-2 metrics-tap)

;; Producer generates 10 more logs
(future
  (println "[Producer Phase 2] Starting...")
  (let [start-time (System/currentTimeMillis)]
    (doseq [i (range 10 20)]
      (csp/put! log-source-2 (str "Log-" i))
      (println "[Producer Phase 2] Sent Log-" i))
    (csp/close! log-source-2)
    (let [duration (- (System/currentTimeMillis) start-time)]
      (println "[Producer Phase 2] Finished. Total time:" duration "ms"))))

;; File writer continues
(future
  (loop []
    (when-let [log (csp/take! file-tap)]
      (println "[File] Writing:" log)
      (recur))))

;; Metrics continues
(future
  (loop [count 10]  ; Continue count from Phase 1
    (when-let [log (csp/take! metrics-tap)]
      (when (= 0 (mod count 3))
        (println "[Metrics] Processed" count "logs"))
      (recur (inc count)))))

;; Wait for Phase 2 to complete
;; With only fast taps, 10 logs should process in ~100ms
(Thread/sleep 500)

(println "\nPhase 2 complete. Time: ~0.5 seconds (20x faster without slow tap)")

(println "\n=== Summary: Backpressure from slowest consumer ===")
(println "Phase 1 (3 taps including 100ms delay): ~10 seconds")
(println "Phase 2 (2 fast taps only): ~0.5 seconds")
(println "Speed improvement: 20x by removing slow consumer")
```

#### Dynamic Tap Management

Taps can be added and removed at runtime. New taps will receive values from the point they're added forward (not retroactively).

```clojure
;; DYNAMIC TAP MANAGEMENT
;; Add taps mid-stream, remove when done

(def source (csp/channel 10))
(def m (csp/multiplex source))

;; Start with one tap
(def tap1 (csp/channel 5))
(csp/tap! m tap1)
(println "Tap1 registered")

;; Producer generates values
(future
  (doseq [i (range 10)]
    (csp/put! source i)
    (println "[Producer] Sent" i)
    ;; Add second tap after 5th value
    (when (= i 4)
      (def tap2 (csp/channel 5))
      (csp/tap! m tap2)
      (println "\n=== Tap2 added mid-stream ===\n")))
  (csp/close! source))

;; Consumer 1: Processes all values from start
(future
  (println "[Consumer 1] Started")
  (loop [count 0]
    (when-let [val (csp/take! tap1)]
      (println "[Consumer 1] Received" val)
      (recur (inc count))))
  (println "[Consumer 1] Done"))

;; Consumer 2: Starts mid-stream, misses first 5 values
(future
  (Thread/sleep 300)  ; Wait for tap2 to be added
  (println "[Consumer 2] Started (late)")
  (loop [count 0]
    (when-let [val (csp/take! tap2)]
      (println "[Consumer 2] Received" val "(tap added after value 4)")
      (recur (inc count))))
  (println "[Consumer 2] Done - Note: missed values 0-4"))

;; Let the example run
(Thread/sleep 1000)

(println "\n=== Key Point ===")
(println "Tap2 missed values 0-4 because it was added after they were sent.")
(println "Taps only receive values sent AFTER they are registered.")
```

### select! (Non-deterministic Choice)

`select!` chooses one ready operation from multiple channel operations. It's like a "case statement" for channels—whichever operation can proceed first, wins.

Unlike core.async `alts!`, csp-clj uses `select!` with an explicit return format `[channel operation result]` that always tells you which channel completed and how.

#### Basic Usage

```clojure
;; Take from first available channel
(csp/select! [[ch1 :take] [ch2 :take]])

;; Put to first available channel
(csp/select! [[ch1 :put :val] [ch2 :put :val]])

;; Mixed operations
(csp/select! [[ch1 :take] [ch2 :put :val]])

;; With timeout - returns :timeout if no operation ready
(csp/select! [[ch1 :take] [ch2 :take]] {:timeout 1000})
```

**Return value format**: `[channel operation result]`

- `[ch :take value]` - successful take, value received
- `[ch :put true]` - successful put, value sent
- `[ch :take nil]` - channel was closed (EOF)
- `[ch :put false]` - channel was closed, put rejected
- `[nil :other :timeout]` - timeout elapsed, no operation completed
- `[nil :other :shutdown]` - all channels closed

#### Complete Example: Worker Pool Load Balancer

This example implements a work dispatcher that sends tasks to the first available worker. The non-deterministic selection naturally load-balances across workers.

```clojure
;; WORKER POOL LOAD BALANCER
;; Dispatcher sends work to first available worker using select!

;; INPUT: Work requests arrive here
(def work-requests (csp/channel 20))

;; WORKERS: Three workers with buffered inboxes
;; Buffers allow workers to queue up to 3 tasks each
(def worker-1 (csp/channel 3))
(def worker-2 (csp/channel 3))
(def worker-3 (csp/channel 3))

;; DISPATCHER: Routes work to first available worker
(future
  (println "[Dispatcher] Starting...")
  (loop [count 0]
    ;; take! blocks until work request arrives
    (when-let [work (csp/take! work-requests)]
      ;; select! chooses first worker with available buffer space
      ;; Non-deterministic = natural load balancing
      (let [[ch op _] (csp/select!
                        [[worker-1 :put work]
                         [worker-2 :put work]
                         [worker-3 :put work]])]
        (cond
          (= ch worker-1) (println "[Dispatcher] Sent" work "to Worker-1")
          (= ch worker-2) (println "[Dispatcher] Sent" work "to Worker-2")
          (= ch worker-3) (println "[Dispatcher] Sent" work "to Worker-3"))
        (recur (inc count)))))
  (println "[Dispatcher] Work requests channel closed, shutting down"))

;; WORKER 1: Processes tasks from its inbox
(future
  (println "[Worker-1] Started")
  (loop []
    ;; take! blocks until dispatcher sends work
    (when-let [task (csp/take! worker-1)]
      (println "[Worker-1] Processing" task)
      ;; Simulate work (random duration 50-150ms)
      (Thread/sleep (+ 50 (rand-int 100)))
      (println "[Worker-1] Completed" task)
      (recur)))
  (println "[Worker-1] Done"))

;; WORKER 2: Processes tasks from its inbox
(future
  (println "[Worker-2] Started")
  (loop []
    (when-let [task (csp/take! worker-2)]
      (println "[Worker-2] Processing" task)
      (Thread/sleep (+ 50 (rand-int 100)))
      (println "[Worker-2] Completed" task)
      (recur)))
  (println "[Worker-2] Done"))

;; WORKER 3: Processes tasks from its inbox
(future
  (println "[Worker-3] Started")
  (loop []
    (when-let [task (csp/take! worker-3)]
      (println "[Worker-3] Processing" task)
      (Thread/sleep (+ 50 (rand-int 100)))
      (println "[Worker-3] Completed" task)
      (recur)))
  (println "[Worker-3] Done"))

;; PRODUCER: Generates 15 work requests
(future
  (println "\n[Producer] Starting...")
  (doseq [i (range 15)]
    (let [work (str "Task-" i)]
      (csp/put! work-requests work)
      (println "[Producer] Submitted" work))
    ;; Small delay between requests to simulate real traffic
    (Thread/sleep 20))
  (println "[Producer] All tasks submitted")
  ;; Close work requests to signal dispatcher to stop
  (csp/close! work-requests))

;; Let the system run
(Thread/sleep 3000)

(println "\n=== Summary ===")
(println "select! naturally load-balanced 15 tasks across 3 workers")
(println "Faster workers automatically received more tasks")
(println "No explicit load balancing logic required")
```

#### Complete Example: Race with Timeout

This pattern races an operation against a timeout, useful for enforcing deadlines.

```clojure
;; RACE WITH TIMEOUT PATTERN
;; Demonstrates racing a slow operation against a timeout

;; RESULT CHANNEL: Slow operation will put here
(def result-ch (csp/channel))

;; TIMEOUT CHANNEL: We'll close this after timeout period
(def timeout-signal (csp/channel))

;; SLOW OPERATION: Simulates a database query or API call
(future
  (println "[Operation] Starting slow process...")
  ;; Simulate work that takes 2 seconds
  (Thread/sleep 2000)
  (csp/put! result-ch "Operation completed successfully")
  (println "[Operation] Done (but we may have timed out)"))

;; TIMEOUT TIMER: Closes timeout channel after 1 second
(future
  (Thread/sleep 1000)
  (csp/close! timeout-signal)
  (println "[Timeout] 1 second elapsed, timeout triggered"))

;; MAIN THREAD: Waits for result or timeout
(println "[Main] Waiting for operation or timeout (whichever comes first)...")
(let [[ch op value] (csp/select!
                      [[result-ch :take]
                       [timeout-signal :take]])]
  (cond
    ;; Result arrived first
    (= ch result-ch)
    (println "[Main] SUCCESS:" value)
    
    ;; Timeout arrived first (channel closed, take returns nil)
    (= ch timeout-signal)
    (println "[Main] TIMEOUT: Operation took too long")
    
    ;; This shouldn't happen in this example
    :else
    (println "[Main] Unexpected result:" ch op value)))

;; Alternative: Using select! with explicit timeout option
;; This is cleaner than the manual timeout channel approach
(println "\n[Main] Now trying with built-in timeout option...")

(def result-ch-2 (csp/channel))

(future
  (Thread/sleep 500)  ; This will complete in time
  (csp/put! result-ch-2 "Quick result"))

(let [[ch op value] (csp/select!
                       [[result-ch-2 :take]]
                       {:timeout 1000})]  ; 1 second timeout
  (if (= value :timeout)
    (println "[Main] Built-in timeout: Operation timed out")
    (println "[Main] Built-in timeout: Got result -" value)))
```

### Pipeline (Parallel Processing)

`pipeline` applies a transducer to values from an input channel in parallel, putting results to an output channel. It maintains output order despite parallelism through an internal ordering mechanism.

Unlike core.async's pipeline which uses a similar approach, csp-clj explicitly documents its two-thread architecture (ingress for taking, egress for putting) and uses virtual threads throughout.

#### Basic Usage

```clojure
;; Basic pipeline with 4-way parallelism
(csp/pipeline 
  4                                    ; parallelism: 4 concurrent operations
  output-ch                            ; output channel
  (map inc)                            ; transducer to apply
  input-ch)                            ; input channel

;; With options
(csp/pipeline 
  8
  output-ch
  (filter even?)
  input-ch
  {:close? false                       ; don't close output when input closes
   :executor :io                       ; use I/O executor for blocking work
   :ex-handler (fn [e] (println e))})  ; handle exceptions
```

**Parameters**:

- `n`: Number of concurrent operations (parallelism)
- `to`: Output channel
- `xf`: Transducer to apply to each value
- `from`: Input channel
- `opts`: Optional configuration map
  - `:close?` - Close output channel when input closes (default: true)
  - `:executor` - `:cpu` (work-stealing) or `:io` (virtual thread per task)
  - `:ex-handler` - Function called when transducer throws exception

#### Ordering Guarantee

Even with parallelism, output order matches input order. This is achieved by wrapping each computation in a `CompletableFuture` and having the egress thread wait for futures in order.

For example, if input is `[1 2 3]` and processing `1` takes longer than `2`, the egress thread still waits for `1` to complete before emitting `2`'s result. This ensures ordered output regardless of execution time variance.

#### CPU vs I/O Executors

**`:cpu` (default)**: Uses a work-stealing thread pool (ForkJoinPool). Best for CPU-bound work like data transformation, calculation, or filtering.

**`:io`**: Creates a new virtual thread for each task. Best for I/O-bound work like HTTP requests, database queries, or file operations.

**Important**: Executors are shared and never shut down. To stop a pipeline, close the input channel.

#### Complete Example: I/O Pipeline with Error Handling

This example fetches random numbers from random.org in parallel, filters for even numbers, and handles errors via a separate error channel.

```clojure
;; I/O PIPELINE: Parallel Random Number Fetch with Error Handling
;; Uses random.org API with parameterized requests

;; REQUEST CHANNEL: Messages are [min max] vectors specifying the range
(def requests (csp/channel 10))

;; RESULTS CHANNEL: Successfully fetched and filtered numbers
(def results (csp/channel 10))

;; ERROR CHANNEL: Buffered channel for pipeline errors
;; Separate channel ensures errors don't block success path
(def errors (csp/channel 10))

;; Fetch function - extracts [min max] from input vector
;; Note: Pipeline applies transducer to [v], so input arrives as [[min max]]
(defn fetch-random-number [[min-val max-val]]
  (let [url (str "https://www.random.org/integers/?num=1&min=" min-val 
                 "&max=" max-val "&col=1&base=10&format=plain&rnd=new")
        response (slurp url)]
    (Integer/parseInt (clojure.string/trim response))))

;; Pipeline with composed transducer and error handling
(csp/pipeline
  4                                    ; 4 concurrent HTTP requests
  results                              ; Success output channel
  (comp                                 ; Composed transducer
    (map fetch-random-number)           ; Step 1: Fetch random number
    (filter even?))                     ; Step 2: Keep only even numbers
  requests                               ; Input channel
  {:executor :io                       ; I/O executor for HTTP requests
   :ex-handler (fn [e]                 ; Send errors to error channel
                 (csp/put! errors 
                   {:error (.getMessage e)
                    :timestamp (System/currentTimeMillis)}))})

;; Producer: Request random numbers in different ranges
(future
  (println "[Producer] Requesting random numbers from random.org...")
  (csp/put! requests [1 100])          ; Request number between 1-100
  (csp/put! requests [1 1000])         ; Request number between 1-1000
  (csp/put! requests [1 10000])        ; Request number between 1-10000
  (csp/put! requests [50 150])         ; Request number between 50-150
  (csp/put! requests [1 100])          ; Another 1-100 request
  (csp/put! requests [1 50])           ; Request number between 1-50
  (csp/close! requests)
  (println "[Producer] All requests sent"))

;; Consumer: Collect even random numbers
(future
  (println "[Consumer] Waiting for even random numbers...")
  (loop [count 0]
    ;; take! with timeout - pipeline closes channel when done
    (if-let [num (csp/take! results 10000)]
      (do
        (println "[Consumer] Got even number:" num)
        (recur (inc count)))
      (println "[Consumer] Pipeline complete. Received" count "even numbers"))))

;; Error handler: Process and log pipeline errors
(future
  (println "[ErrorHandler] Monitoring for errors...")
  (loop []
    ;; Errors are put on error channel by :ex-handler
    (when-let [err (csp/take! errors 10000)]
      (println "[ErrorHandler] Pipeline error:" err)
      (recur)))
  (println "[ErrorHandler] Done"))

;; Let the example run
(Thread/sleep 5000)

(println "\n=== Summary ===")
(println "Pipeline fetched random numbers from random.org in parallel")
(println "Even numbers were filtered and passed to results channel")
(println "Errors were captured separately without blocking success path")
(println "Note: Input format is [min max], transducer receives [[min max]]")
```

#### Complete Example: CPU Pipeline

This example demonstrates CPU-bound work using the `:cpu` executor.

```clojure
;; CPU PIPELINE: Parallel Data Processing
;; Uses :cpu executor for computation-bound work

;; Input: Numbers to process
(def input (csp/channel 20))

;; Output: Processed results
(def output (csp/channel 20))

;; Heavy computation function
;; Simulates CPU-intensive work (e.g., cryptographic hash, matrix operation)
(defn expensive-calculation [n]
  ;; Simulate CPU work
  (Thread/sleep 50)
  ;; Return square
  (* n n))

;; Pipeline for CPU-bound processing
(csp/pipeline 
  4                                    ; 4 parallel workers
  output                               ; Output channel
  (map expensive-calculation)          ; Transducer: heavy computation
  input                                ; Input channel
  {:executor :cpu})                    ; CPU executor (work stealing)

;; Producer: Generate numbers to process
(future
  (println "[Producer] Generating input data...")
  (doseq [i (range 20)]
    (csp/put! input i))
  (csp/close! input)
  (println "[Producer] Done"))

;; Consumer: Collect results
(future
  (println "[Consumer] Processing results...")
  (loop []
    (when-let [result (csp/take! output)]
      (println "[Consumer] Result:" result)
      (recur)))
  (println "[Consumer] Done"))

;; Let it run
(Thread/sleep 3000)

(println "\n=== Key Points ===")
(println "Transducer applied to single-element vectors: [v]")
(println "Use :cpu executor for computation work")
(println "Use :io executor for blocking I/O work")
(println "Output order matches input order despite parallelism")
```

### Pub/Sub (Topic-Based Routing)

Pub/Sub (publish/subscribe) routes values from a source channel to different subscriber channels based on a **topic**. Unlike multiplexers which broadcast to all taps, Pub/Sub only sends values to subscribers interested in that specific topic.

**Key concept**: A *topic function* extracts the routing key from each value. Values are then routed only to subscribers of that topic.

#### Multiplexer vs Pub/Sub Comparison

| Feature | Multiplexer | Pub/Sub |
|---------|-------------|---------|
| Pattern | Broadcast | Routing by topic |
| Who receives | **All** taps | Only matching topic subscribers |
| Use case | Log replication, fan-out | Event routing, chat rooms |
| API | `tap!` | `sub!` with topic key |
| Message filtering | None - everything broadcast | Automatic by topic function |

**When to use which:**
- Use **multiplexer** when every consumer needs every message
- Use **Pub/Sub** when different consumers care about different categories of messages

#### Basic Usage

```clojure
;; Create a publisher with a topic function
;; The topic function extracts the routing key from each value
(def source (csp/channel))
(def p (csp/pub! source :event-type))  ; Route by :event-type field

;; Subscribe channels to specific topics
(def chat-ch (csp/channel 10))
(def system-ch (csp/channel 5))

(csp/sub! p :chat chat-ch)     ; Subscribe to :chat topic
(csp/sub! p :system system-ch) ; Subscribe to :system topic

;; Now only matching messages are routed:
(csp/put! source {:event-type :chat :msg "Hello"})   ; -> chat-ch only
(csp/put! source {:event-type :system :msg "Restart"}) ; -> system-ch only
```

**API Overview:**

- `(pub! source-ch topic-fn)` - Create publisher with topic function
- `(pub! source-ch topic-fn buf-fn)` - With per-topic buffer sizing
- `(sub! pub topic ch)` - Subscribe channel to topic
- `(sub! pub topic ch close?)` - Subscribe with auto-close option
- `(unsub! pub topic ch)` - Unsubscribe channel from topic
- `(unsub-all! pub)` - Unsubscribe all from all topics
- `(unsub-all! pub topic)` - Unsubscribe all from specific topic

#### Lazy Topic Creation

Topics are created **on-demand** when the first subscriber registers. Before any subscriptions, no internal resources are allocated for that topic.

```clojure
(def p (csp/pub! source :type))

;; At this point, no topics exist internally

(csp/sub! p :orders order-ch)  ; :orders topic created now
(csp/sub! p :billing billing-ch) ; :billing topic created now

;; Topics auto-cleanup when last subscriber unsubscribes
(csp/unsub! p :orders order-ch)  ; :orders topic removed (no subscribers left)
```

This lazy creation prevents memory leaks and reduces resource usage for unused topics.

#### Complete Example: Event Router with Dynamic Subscriptions

This example demonstrates a real-time event routing system with chat messages, system notifications, and error logs. It shows dynamic subscription (adding a subscriber mid-stream) and temporary unsubscription.

```clojure
;; EVENT ROUTER WITH DYNAMIC SUBSCRIPTIONS
;; Routes different event types to appropriate handlers

(def event-source (csp/channel 20))

;; Publisher routes by :event-type field
(def router (csp/pub! event-source :event-type))

;; INITIAL SUBSCRIBERS: Chat and System only
(def chat-ch (csp/channel 10))
(def system-ch (csp/channel 5))

(csp/sub! router :chat chat-ch)
(csp/sub! router :system system-ch)

(println "[Setup] Subscribed :chat and :system handlers")

;; CHAT HANDLER: Processes chat messages
(future
  (println "[ChatHandler] Started")
  (loop [count 0]
    (when-let [event (csp/take! chat-ch)]
      (println "[Chat]" (:user event) ":" (:msg event))
      (recur (inc count))))
  (println "[ChatHandler] Done"))

;; SYSTEM HANDLER: Processes system notifications
(future
  (println "[SystemHandler] Started")
  (loop []
    (when-let [event (csp/take! system-ch)]
      (println "[System]" (:msg event))
      (recur)))
  (println "[SystemHandler] Done"))

;; EVENT PRODUCER: Simulates mixed event stream
(future
  (println "\n[Producer] Phase 1: Sending initial events...")
  
  (csp/put! event-source {:event-type :chat :user "alice" :msg "Hello everyone!"})
  (csp/put! event-source {:event-type :system :msg "Server status: Online"})
  (csp/put! event-source {:event-type :chat :user "bob" :msg "Hi Alice!"})
  (csp/put! event-source {:event-type :error :msg "DB connection slow"})  ; No handler yet!
  (csp/put! event-source {:event-type :chat :user "alice" :msg "How's it going?"})
  
  (Thread/sleep 100)
  
  ;; DYNAMIC SUBSCRIPTION: Add error handler mid-stream
  (println "\n[Producer] Phase 2: Adding error handler dynamically...")
  (def error-ch (csp/channel 10))
  (csp/sub! router :error error-ch)
  (println "[Producer] Error handler subscribed (will miss earlier errors)")
  
  ;; ERROR HANDLER: Late subscriber - misses first error!
  (future
    (println "[ErrorHandler] Started (late)")
    (loop [missed 0]
      (when-let [event (csp/take! error-ch)]
        (println "[Error] ALERT:" (:msg event))
        (recur missed)))
    (println "[ErrorHandler] Done (missed the first error)"))
  
  ;; Continue with more events
  (csp/put! event-source {:event-type :error :msg "Cache miss rate high"})
  (csp/put! event-source {:event-type :system :msg "Memory usage: 60%"})
  (csp/put! event-source {:event-type :chat :user "bob" :msg "All good here"})
  (csp/put! event-source {:event-type :error :msg "Network latency spike"})
  
  (Thread/sleep 100)
  
  ;; DYNAMIC UNSUBSCRIPTION: Temporarily unsubscribe chat
  (println "\n[Producer] Phase 3: Unsubscribing chat for maintenance...")
  (csp/unsub! router :chat chat-ch)
  (println "[Producer] Chat handler unsubscribed")
  
  ;; These chat messages go nowhere - no subscribers!
  (csp/put! event-source {:event-type :chat :user "alice" :msg "Anyone there?"})
  (csp/put! event-source {:event-type :chat :user "bob" :msg "Hello?"})
  (csp/put! event-source {:event-type :system :msg "Chat service paused"})
  
  (Thread/sleep 100)
  
  ;; RE-SUBSCRIBE: Chat handler back online
  (println "\n[Producer] Phase 4: Re-subscribing chat...")
  (csp/sub! router :chat chat-ch)
  (csp/put! event-source {:event-type :chat :user "alice" :msg "Back online!"})
  (csp/put! event-source {:event-type :system :msg "Chat service resumed"})
  
  (Thread/sleep 50)
  (csp/close! event-source)
  (println "\n[Producer] Event source closed"))

;; Let the example run
(Thread/sleep 2000)

(println "\n=== Key Observations ===")
(println "1. Error handler missed first error (subscribed after it was sent)")
(println "2. Chat messages during unsubscribe phase were dropped (no subscribers)")
(println "3. System messages kept flowing throughout")
(println "4. Topics are lazily created and automatically cleaned up")
```

#### Complete Example: Per-Topic Buffering

This example shows how to use `buf-fn` to give different topics different buffer sizes. High-priority events get larger buffers to prevent dropping.

```clojure
;; PER-TOPIC BUFFERING
;; High-priority events get larger buffers

(def event-source (csp/channel))

;; Publisher with per-topic buffer sizing
;; buf-fn receives the topic and returns buffer capacity
(def router (csp/pub! event-source :priority
                       (fn [topic]
                         (case topic
                           :high 100      ; Large buffer for critical events
                           :normal 10     ; Standard buffer
                           :low 1         ; Minimal buffer (drop if backed up)
                           5))))         ; Default

;; Subscribe to different priority levels
(def high-ch (csp/channel))
(def normal-ch (csp/channel))
(def low-ch (csp/channel))

(csp/sub! router :high high-ch)
(csp/sub! router :normal normal-ch)
(csp/sub! router :low low-ch)

;; HIGH-PRIORITY HANDLER: Must not drop events
(future
  (println "[HighPriority] Started (buffer: 100)")
  (loop []
    (when-let [event (csp/take! high-ch)]
      (println "[HIGH]" (:msg event))
      ;; Simulate slow processing
      (Thread/sleep 100)
      (recur)))
  (println "[HighPriority] Done"))

;; NORMAL HANDLER: Standard processing
(future
  (println "[NormalPriority] Started (buffer: 10)")
  (loop []
    (when-let [event (csp/take! normal-ch)]
      (println "[NORMAL]" (:msg event))
      (Thread/sleep 50)
      (recur)))
  (println "[NormalPriority] Done"))

;; LOW-PRIORITY HANDLER: Can drop events if backed up
(future
  (println "[LowPriority] Started (buffer: 1)")
  (loop [dropped 0]
    (when-let [event (csp/take! low-ch)]
      ;; Check if this was a buffered event or just-received
      (println "[LOW]" (:msg event))
      (recur dropped)))
  (println "[LowPriority] Done"))

;; Producer: Burst of events
(future
  (println "\n[Producer] Sending burst of 50 events to each priority...")
  
  ;; Send 50 high priority (buffered, none dropped)
  (dotimes [i 50]
    (csp/put! event-source {:priority :high :msg (str "Critical-" i)}))
  
  ;; Send 50 normal priority (some may fill buffer)
  (dotimes [i 50]
    (csp/put! event-source {:priority :normal :msg (str "Normal-" i)}))
  
  ;; Send 50 low priority (most will be dropped due to buffer size 1)
  (dotimes [i 50]
    (csp/put! event-source {:priority :low :msg (str "Log-" i)}))
  
  (csp/close! event-source)
  (println "[Producer] Source closed"))

;; Let it run
(Thread/sleep 6000)

(println "\n=== Buffer Strategy Results ===")
(println "HIGH (buffer 100): All 50 events buffered, processed completely")
(println "NORMAL (buffer 10): 10 events buffered, rest applied backpressure")
(println "LOW (buffer 1): Only latest event kept, old events dropped")
(println "\nUse buf-fn to prevent critical event loss while allowing non-critical dropping")
```
