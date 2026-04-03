(ns csp-clj.channels.pipeline
  "Parallel pipeline processing for channels.
   
   Provides parallel transducer application with ordering guarantees.
   Values flow from input channel through transducer to output channel
   with configurable parallelism.
   
   Key Concepts for New Developers:
   - Parallelism: Multiple values processed concurrently (n workers)
   - Ordering: Output order matches input order (via CompletableFuture)
   - Backpressure: Limited by buffer size of jobs channel (size n)
   - Executor choice: :cpu for computation, :io for I/O-bound work
   
   Algorithm Overview:
   1. Two virtual threads: ingress (takes) and egress (puts)
   2. Jobs channel (buffered size n) limits parallelism
   3. Each job wrapped in CompletableFuture for ordering
   4. Transducer applied in executor threads
   5. Egress thread puts results in order, maintains backpressure
   
   Called by: csp-clj.channels/pipeline, csp-clj.core/pipeline"
  (:require
   [csp-clj.protocols.channel :as channel-protocol]
   [csp-clj.channels.buffered :as buffered])
  (:import
   [java.util.concurrent Executors ExecutorService CompletableFuture]))

(set! *warn-on-reflection* true)

;; PIPELINE - PARALLEL TRANSDUCER PROCESSING
;;
;; Pipeline provides parallel processing of channel values through a transducer,
;; with ordering guarantees. This is the csp-clj equivalent of core.async/pipeline.
;;
;; ARCHITECTURE
;;
;; Pipeline uses a two-virtual-thread architecture:
;;
;;   Ingress Thread                    Egress Thread
;;   --------------                    -------------
;;   Takes from 'from'                 Takes from jobs
;;   Creates CompletableFuture         Gets results via .get
;;   Submits work to executor          Puts results to 'to'
;;   Limits via jobs channel           Maintains ordering
;;
;; The threads communicate via a buffered 'jobs' channel (size = parallelism).
;; This channel provides both work distribution and backpressure.
;;
;; ORDERING GUARANTEE VIA COMPLETABLEFUTURE
;;
;; Output order matches input order through CompletableFuture chaining:
;;
;;   Input:  [v1, v2, v3]  →  Futures: [f1, f2, f3]  →  Output: [r1, r2, r3]
;;
;; How it works:
;; 1. Each input value gets a CompletableFuture wrapper
;; 2. Futures are put onto jobs channel in strict input order
;; 3. Egress thread takes futures in order and calls .get()
;; 4. .get() blocks until THAT SPECIFIC computation completes
;; 5. Even if v2 finishes before v1, egress waits for f1 before f2
;;
;; This ensures ordered output regardless of execution time variance.
;;
;; BACKPRESSURE MECHANISM
;;
;; Backpressure is provided by the jobs channel (buffered, size = n):
;;
;;   ;; When jobs channel is full (n futures pending)
;;   (put! jobs new-future)  ; BLOCKS ingress thread
;;
;; This naturally limits in-flight work to 'n' parallel operations.
;; If egress is slow, jobs fills up, ingress blocks, backpressure propagates.
;;
;; EXECUTOR STRATEGY
;;
;; :cpu - Work-stealing pool (ForkJoinPool.commonPool)
;;        Best for CPU-bound work (computation, transformation)
;;        Shares threads across all :cpu pipelines
;;
;; :io  - Virtual thread per task executor
;;        Best for I/O-bound work (network, disk, sleeping)
;;        Creates new virtual thread for each task
;;
;; IMPORTANT: Executors are shared (defonce) and NEVER SHUTDOWN.
;; To "stop" a pipeline, close the input or output channels.
;; The threads will exit gracefully and channels will clean up.
;;
;; SHUTDOWN COORDINATION
;;
;; Graceful shutdown when input closes:
;; 1. Ingress takes nil from 'from', stops submitting work
;; 2. Ingress closes jobs channel (signals egress)
;; 3. Egress drains remaining futures, puts results
;; 4. Egress takes nil from jobs, exits
;;
;; Early termination when output closes:
;; 1. Egress put! returns false ('to' closed)
;; 2. Egress closes jobs channel (signals ingress)
;; 3. Ingress blocked on put! to jobs wakes, sees jobs closed
;; 4. Ingress exits, dropping pending input
;;
;; TRANSDUCER INPUT FORMAT
;;
;; The transducer is applied to a SINGLE-ELEMENT VECTOR: [v]
;; This matters for stateful transducers (partition-all, dedupe, etc.)
;; that maintain state across inputs.
;;
;; See also: csp-clj.channels.buffered, java.util.concurrent.CompletableFuture

;; Shared work-stealing executor for CPU-bound tasks.
;; Used by default for pipeline processing.
;;
;; WHY SHARED (defonce):
;; - Prevents thread pool explosion when many pipelines created
;; - Reuses ForkJoinPool.commonPool threads efficiently
;; - Lifecycle: Created once, lives until JVM exit
;;
;; WHY NEVER SHUTDOWN:
;; - Shutdown would break other running pipelines
;; - To stop processing: close input/output channels
;; - Threads exit gracefully, executor remains for reuse
(defonce ^:private cpu-executor (Executors/newWorkStealingPool))

;; Shared virtual thread executor for I/O-bound tasks.
;; Used when :io executor option specified.
;;
;; Virtual threads are cheap to create, but we still share
;; the executor to manage lifecycle consistently with :cpu.
(defonce ^:private io-executor (Executors/newVirtualThreadPerTaskExecutor))

(defn- get-executor
  "Returns the appropriate executor based on option.
   
   Algorithm: Maps keywords to shared executors, or returns
   custom ExecutorService if provided.
   
   Parameters:
     - executor-opt: :cpu, :io, or ExecutorService instance
   
   Returns:
     ExecutorService instance
   
   Called by: pipeline"
  ^ExecutorService [executor-opt]
  (case executor-opt
    :cpu cpu-executor
    :io io-executor
    (if (instance? ExecutorService executor-opt)
      executor-opt
      cpu-executor)))

(defn- default-ex-handler
  "Default exception handler for pipeline errors.
   
   Delegates to thread's uncaught exception handler.
   Returns nil to signal no output produced.
   
   Parameters:
     - ex: the exception that occurred
   
   Returns:
     nil
   
   Called by: pipeline (when no custom handler provided)"
  [ex]
  (let [t (Thread/currentThread)]
    (-> t .getUncaughtExceptionHandler (.uncaughtException t ex)))
  nil)

(defn pipeline
  "Takes elements from the from channel and supplies them to the to
   channel, subject to the transducer xf, with parallelism n.

   Because it is parallel, the transducer will be applied independently
   to each element. Outputs will be returned in order relative to the inputs.

   Algorithm:
   - Ingress thread: Takes from 'from', submits work as CompletableFuture
   - Jobs channel (size n): Limits in-flight work, provides backpressure
   - Executor threads: Apply transducer to each value
   - Egress thread: Takes futures in order, puts results to 'to'

   ORDERING GUARANTEE

   Output order matches input order via CompletableFuture. Each input value
   is wrapped in a future, and the egress thread waits for futures in order.
   Even if v2 computes faster than v1, v1's result is emitted first.

   BACKPRESSURE

   The jobs channel (buffered with size n) provides natural backpressure.
   When full, the ingress thread blocks on put!, stalling the input.

   TRANSDUCER INPUT FORMAT

   The transducer xf is applied to SINGLE-ELEMENT VECTORS: [v]
   This matters for stateful transducers (partition-all, dedupe, etc.)
   that maintain state across inputs.

   EXCEPTION HANDLING

   Both ingress and egress threads catch Throwable (not just Exception).
   This ensures Error types (OutOfMemoryError, StackOverflowError) are
   reported via the :ex-handler and channels are properly cleaned up.

   SHARED EXECUTORS WARNING

   Executors are shared across pipelines and NEVER shutdown.
   To stop a pipeline, close its input or output channels.

   Options:
   - :close? (default true) - Close the to channel when from channel closes
   - :executor (default :cpu) - :cpu (work stealing) or :io (virtual threads)
   - :ex-handler - Function to handle exceptions during transduction

   Parameters:
     - n: parallelism level (number of concurrent operations)
     - to: output channel
     - xf: transducer to apply
     - from: input channel
     - opts: optional configuration map

   Returns:
     The output channel 'to'

   Example:
     (pipeline 4 out-ch (map inc) in-ch)
     (pipeline 8 out-ch (filter odd?) in-ch {:close? false})
     (pipeline 4 out-ch (map heavy-calc) in-ch {:executor :io})

   See also:
     - csp-clj.channels/pipeline for high-level API
     - csp-clj.core/pipeline for convenience wrapper
     - java.util.concurrent.CompletableFuture for ordering mechanism"
  ([n to xf from]
   (pipeline n to xf from nil))
  ([n to xf from {:keys [close? executor ex-handler]
                  :or {close? true
                       executor :cpu
                       ex-handler default-ex-handler}}]
   (let [exec (get-executor executor)
         jobs (buffered/create n)]

      ;; EGRESS THREAD: Takes futures in order, puts results in order
     (Thread/startVirtualThread
      (fn []
        (try
          (loop []
             ;; BLOCKING: Wait for next CompletableFuture from jobs channel
            (let [future-job (channel-protocol/take! jobs)]
              (if (nil? future-job)
                 ;; Jobs channel closed - ingress finished, drain complete
                nil
                (let [^CompletableFuture f future-job
                       ;; BLOCKING: Wait for THIS SPECIFIC computation to complete
                       ;; This maintains ordering: we wait for f1 even if f2 finished first
                      results (.get f)
                       ;; Transducer applied to [v] produces 0-N results (sequence)
                      keep-going? (loop [rs (seq results)]
                                    (if rs
                                      (let [v (first rs)]
                                        (if (nil? v)
                                           ;; Skip nil values from transducer
                                          (recur (next rs))
                                          (if (channel-protocol/put! to v)
                                             ;; Successfully put to output, continue
                                            (recur (next rs))
                                             ;; Output channel closed - signal early termination
                                            false)))
                                      true))]
                  (if keep-going?
                    (recur)
                     ;; Output closed: close jobs to signal ingress to stop immediately
                    (channel-protocol/close! jobs))))))
           ;; EXCEPTION HANDLING FIX: Catch Throwable (not just Exception)
           ;; Catching Throwable ensures:
           ;; 1. Error types (OOM, StackOverflow) are reported via ex-handler
           ;; 2. Channels are always closed properly even on fatal errors
           ;; 3. User has visibility into all failure modes
           ;; 4. Symmetry with ingress thread exception handling
          (catch Throwable t
            (ex-handler t))
          (finally
             ;; Always cleanup: close jobs and optionally close output
            (channel-protocol/close! jobs)
            (when close?
              (channel-protocol/close! to))))))

      ;; INGRESS THREAD: Takes from input, creates futures, submits work
     (Thread/startVirtualThread
      (fn []
        (try
          (loop []
             ;; BLOCKING: Wait for input from 'from' channel
            (let [v (channel-protocol/take! from)]
              (if (nil? v)
                 ;; Input channel closed - stop accepting new work
                nil
                (let [f (CompletableFuture.)]
                   ;; BLOCKING POINT: Put future onto jobs queue
                   ;; This limits parallelism to n because jobs is buffered with size n
                   ;; When full, this blocks, applying backpressure to the source
                  (if (channel-protocol/put! jobs f)
                    (do
                       ;; Submit transducer work to executor
                       ;; The transducer is applied to [v] (single-element vector)
                       ;; This matters for stateful transducers (partition-all, dedupe, etc.)
                      (.execute exec
                                (fn []
                                  (try
                                     ;; Apply transducer xf to single-element vector [v]
                                    (let [results (into [] xf [v])]
                                       ;; Complete future with results (0-N values)
                                      (.complete f results))
                                     ;; Catch Throwable (not just Exception) for consistency
                                     ;; Complete future with empty vector so egress doesn't hang
                                    (catch Throwable e
                                      (try
                                        (ex-handler e)
                                        (finally
                                           ;; MUST complete future even on error
                                           ;; Otherwise egress thread would block forever on .get
                                          (.complete f [])))))))
                      (recur))
                     ;; Jobs channel was closed by egress (output channel closed)
                     ;; Exit immediately, dropping any pending input
                    nil)))))
           ;; Catch all exceptions/errors for reporting
          (catch Throwable t
            (ex-handler t))
          (finally
             ;; Signal egress to drain and exit
            (channel-protocol/close! jobs)))))
     to)))
