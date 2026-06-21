(ns csp-clj.performance.channels-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [criterium.core :as criterium]
   [csp-clj.core :as csp]
   [clojure.core.async :as a]))

(set! *warn-on-reflection* true)

(def iterations 100000)

(defn- run-csp-benchmark
  [n capacity]
  (let [ch (if capacity (csp/channel capacity) (csp/channel))]
    (future
      (dotimes [_ n]
        (csp/put! ch :val)))
    (dotimes [_ n]
      (csp/take! ch))))

(defn- run-async-benchmark
  [n capacity]
  (let [ch (if capacity (a/chan capacity) (a/chan))]
    (a/thread
      (dotimes [_ n]
        (a/>!! ch :val)))
    (dotimes [_ n]
      (a/<!! ch))))

(deftest ^:performance unbuffered-performance-tests
  (testing "Unbuffered Channel Performance (100k items)"
    (println "\n--- BENCHMARKING: csp-clj unbuffered ---")
    (criterium/quick-bench (run-csp-benchmark iterations nil))

    (println "\n--- BENCHMARKING: core.async unbuffered ---")
    (criterium/quick-bench (run-async-benchmark iterations nil))

    ;; A dummy assertion so the test runner reports something
    (is true "Benchmarks completed successfully")))

(deftest ^:performance buffered-performance-tests
  (testing "Buffered Channel Performance (Capacity: 5, 100k items)"
    (println "\n--- BENCHMARKING: csp-clj buffered (5) ---")
    (criterium/quick-bench (run-csp-benchmark iterations 5))

    (println "\n--- BENCHMARKING: core.async buffered (5) ---")
    (criterium/quick-bench (run-async-benchmark iterations 5))

    ;; A dummy assertion so the test runner reports something
    (is true "Benchmarks completed successfully")))

(deftest ^:performance buffered-capacity-scaling-performance-tests
  (testing "Buffered Channel Capacity Scaling (varying capacity, 25k items per config)"
    ;; Reduced iterations (25k per config) to keep total runtime bounded
    ;; across 4 capacity levels x 2 libraries x criterium's multiple evals.
    (let [scaling-iterations 25000]
      (doseq [capacity [1 5 100 1000]]
        (println (str "\n--- BENCHMARKING (capacity " capacity "): csp-clj buffered ---"))
        (criterium/quick-bench (run-csp-benchmark scaling-iterations capacity))
        (println (str "\n--- BENCHMARKING (capacity " capacity "): core.async buffered ---"))
        (criterium/quick-bench (run-async-benchmark scaling-iterations capacity))))
    (is true "Benchmarks completed successfully")))
