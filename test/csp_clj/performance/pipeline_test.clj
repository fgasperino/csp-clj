(ns csp-clj.performance.pipeline-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [criterium.core :as criterium]
   [csp-clj.core :as csp]
   [clojure.core.async :as a]))

(set! *warn-on-reflection* true)

(def iterations 100000)

(defn- run-csp-pipeline
  [n parallelism]
  (let [from (csp/channel n)
        to (csp/channel 100)]

    (dotimes [i n]
      (csp/put! from i))
    (csp/close! from)

    (csp/pipeline parallelism to (map inc) from)

    (loop []
      (when-let [_ (csp/take! to)]
        (recur)))))

(defn- run-async-pipeline
  [n parallelism]
  (let [from (a/chan n)
        to (a/chan 100)]

    (dotimes [i n]
      (a/>!! from i))
    (a/close! from)

    (a/pipeline parallelism to (map inc) from)

    (loop []
      (when-let [v (a/<!! to)]
        (recur)))))

(deftest ^:performance pipeline-performance-tests
  (testing "Pipeline Parallel Processing (parallelism 4, identity+inc transducer, 100k items)"
    (println "\n--- BENCHMARKING: csp-clj pipeline ---")
    (criterium/quick-bench (run-csp-pipeline iterations 4))

    (println "\n--- BENCHMARKING: core.async pipeline ---")
    (criterium/quick-bench (run-async-pipeline iterations 4))

    (is true "Benchmarks completed successfully")))

(deftest ^:performance pipeline-scaling-performance-tests
  (testing "Pipeline Scaling (varying parallelism, 25k items per config)"
    ;; Reduced iterations (25k per config) to keep total runtime bounded
    ;; across 4 parallelism levels x 2 libraries x criterium's multiple evals.
    (let [scaling-iterations 25000]
      (doseq [parallelism [1 2 4 8]]
        (println (str "\n--- BENCHMARKING (parallelism " parallelism "): csp-clj pipeline ---"))
        (criterium/quick-bench (run-csp-pipeline scaling-iterations parallelism))
        (println (str "\n--- BENCHMARKING (parallelism " parallelism "): core.async pipeline ---"))
        (criterium/quick-bench (run-async-pipeline scaling-iterations parallelism))))
    (is true "Benchmarks completed successfully")))