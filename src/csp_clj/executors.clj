(ns csp-clj.executors
  (:import
   [java.util.concurrent Executors]))

(set! *warn-on-reflection* true)

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
(defonce cpu-executor (Executors/newWorkStealingPool))

;; Shared virtual thread executor for I/O-bound tasks.
;; Used when :io executor option specified.
;;
;; Virtual threads are cheap to create, but we still share
;; the executor to manage lifecycle consistently with :cpu.
(defonce io-executor (Executors/newVirtualThreadPerTaskExecutor))
