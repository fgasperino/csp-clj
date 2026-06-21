(ns csp-clj.performance.into-chan-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [criterium.core :as criterium]
   [csp-clj.core :as csp]
   [clojure.core.async :as a]))

(set! *warn-on-reflection* true)

(def iterations 100000)

(defn- run-csp-into-chan
  [n]
  (let [ch (csp/channel 100)]
    (csp/into-chan! ch (range n))
    (loop []
      (when-let [v (csp/take! ch)]
        (recur)))))

(defn- run-async-onto-chan
  [n]
  (let [ch (a/chan 100)]
    (a/onto-chan ch (range n))
    (loop []
      (when-let [v (a/<!! ch)]
        (recur)))))

(deftest ^:performance into-chan-performance-tests
  (testing "into-chan! vs onto-chan (100k items, buffer capacity 100)"
    (println "\n--- BENCHMARKING: csp-clj into-chan! ---")
    (criterium/quick-bench (run-csp-into-chan iterations))

    (println "\n--- BENCHMARKING: core.async onto-chan ---")
    (criterium/quick-bench (run-async-onto-chan iterations))

    (is true "Benchmarks completed successfully")))