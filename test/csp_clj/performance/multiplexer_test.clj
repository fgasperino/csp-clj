(ns csp-clj.performance.multiplexer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [criterium.core :as criterium]
   [csp-clj.core :as csp]
   [csp-clj.channels :as channels]
   [clojure.core.async :as a]))

(set! *warn-on-reflection* true)

(def iterations 100000)
(def num-taps 5)
(def buffer-size 10)

(defn- run-csp-multiplexer
  [n taps capacity]
  (let [source (csp/channel capacity)
        mult (channels/multiplex source)
        tap-chs (vec (repeatedly taps #(csp/channel capacity)))]

    (doseq [ch tap-chs]
      (channels/tap! mult ch))

    ;; Start consumers for all taps
    (doseq [ch tap-chs]
      (future
        (dotimes [_ n]
          (csp/take! ch))))

    ;; Producer
    (dotimes [_ n]
      (csp/put! source :val))

    ;; Clean up
    (csp/close! source)))

(defn- run-async-multiplexer
  [n taps capacity]
  (let [source (a/chan capacity)
        mult (a/mult source)
        tap-chs (vec (repeatedly taps #(a/chan capacity)))]

    (doseq [ch tap-chs]
      (a/tap mult ch))

    ;; Start consumers for all taps
    (doseq [ch tap-chs]
      (a/thread
        (dotimes [_ n]
          (a/<!! ch))))

    ;; Producer
    (dotimes [_ n]
      (a/>!! source :val))

    ;; Clean up
    (a/close! source)))

(deftest ^:performance multiplexer-performance-tests
  (testing "Multiplexer Broadcast (1 source -> 5 buffered taps, 100k items)"
    (println "\n--- BENCHMARKING: csp-clj multiplexer ---")
    (criterium/quick-bench (run-csp-multiplexer iterations num-taps buffer-size))

    (println "\n--- BENCHMARKING: core.async mult ---")
    (criterium/quick-bench (run-async-multiplexer iterations num-taps buffer-size))

    (is true "Benchmarks completed successfully")))
