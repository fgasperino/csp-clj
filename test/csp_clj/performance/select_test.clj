(ns csp-clj.performance.select-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [criterium.core :as criterium]
   [csp-clj.core :as csp]
   [clojure.core.async :as a]))

(set! *warn-on-reflection* true)

(def iterations 100000)

(defn- run-csp-select-fast-path
  [n num-channels]
  (let [channels (vec (repeatedly num-channels #(csp/channel n)))
        ops (mapv #(vector % :take) channels)]
    ;; Pre-fill all channels
    (doseq [ch channels]
      (dotimes [_ n]
        (csp/put! ch :val)))

    ;; Drain using select!
    (dotimes [_ (* n num-channels)]
      (csp/select! ops))))

(defn- run-async-alts-fast-path
  [n num-channels]
  (let [channels (vec (repeatedly num-channels #(a/chan n)))
        ops channels] ;; alts!! takes just the channel for takes
    ;; Pre-fill all channels
    (doseq [ch channels]
      (dotimes [_ n]
        (a/>!! ch :val)))

    ;; Drain using alts!!
    (dotimes [_ (* n num-channels)]
      (a/alts!! ops))))

(deftest ^:performance select-fast-path-performance-tests
  (testing "select! vs alts!! Fast Path (5 channels, 100k items)"
    (println "\n--- BENCHMARKING: csp-clj select! (fast path) ---")
    (criterium/quick-bench (run-csp-select-fast-path iterations 5))

    (println "\n--- BENCHMARKING: core.async alts!! (fast path) ---")
    (criterium/quick-bench (run-async-alts-fast-path iterations 5))

    (is true "Benchmarks completed successfully")))

(defn- run-csp-select-slow-path
  [n num-channels]
  (let [channels (vec (repeatedly num-channels csp/channel))
        ops (mapv #(vector % :take) channels)]

    ;; Producer thread that slowly drips values into random channels
    (future
      (dotimes [_ n]
        (let [target-ch (nth channels (rand-int num-channels))]
          ;; Use sleep instead of yield to ensure we reliably hit the slow path
          (Thread/sleep 1)
          (csp/put! target-ch :val))))

    ;; Consumer thread that uses select! to wait
    (dotimes [_ n]
      (csp/select! ops))))

(defn- run-async-alts-slow-path
  [n num-channels]
  (let [channels (vec (repeatedly num-channels a/chan))
        ops channels]

    (a/thread
      (dotimes [_ n]
        (let [target-ch (nth channels (rand-int num-channels))]
          (Thread/sleep 1)
          (a/>!! target-ch :val))))

    (dotimes [_ n]
      (a/alts!! ops))))

(deftest ^:performance select-slow-path-performance-tests
  (testing "select! vs alts!! Slow Path (5 unbuffered channels, 100 items)"
    ;; Use fewer iterations for slow path as it involves actual blocking
    (let [slow-iterations 100]
      (println "\n--- BENCHMARKING: csp-clj select! (slow path) ---")
      (criterium/quick-bench (run-csp-select-slow-path slow-iterations 5))

      (println "\n--- BENCHMARKING: core.async alts!! (slow path) ---")
      (criterium/quick-bench (run-async-alts-slow-path slow-iterations 5))

      (is true "Benchmarks completed successfully"))))

(deftest ^:performance select-channel-scaling-performance-tests
  (testing "select! vs alts!! Fast Path Scaling (varying channel count, 10k items per channel)"
    ;; Reduced iterations (10k per channel per config) to keep total runtime
    ;; bounded across 4 channel counts x 2 libraries x criterium's evals.
    ;; Total selects scale with channel count: 10k x 2 = 20k, 10k x 50 = 500k.
    (let [scaling-iterations 10000]
      (doseq [num-channels [2 5 10 50]]
        (println (str "\n--- BENCHMARKING (" num-channels " channels): csp-clj select! (fast path) ---"))
        (criterium/quick-bench (run-csp-select-fast-path scaling-iterations num-channels))
        (println (str "\n--- BENCHMARKING (" num-channels " channels): core.async alts!! (fast path) ---"))
        (criterium/quick-bench (run-async-alts-fast-path scaling-iterations num-channels))))
    (is true "Benchmarks completed successfully")))
