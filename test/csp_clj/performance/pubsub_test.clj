(ns csp-clj.performance.pubsub-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [criterium.core :as criterium]
   [csp-clj.core :as csp]
   [csp-clj.channels :as channels]
   [clojure.core.async :as a]))

(set! *warn-on-reflection* true)

(def iterations 100000)

(defn- run-csp-pubsub
  [n num-topics]
  (let [per-topic (int (/ n num-topics))
        total (* per-topic num-topics)
        source (csp/channel 1000)
        p (channels/pub! source :topic)
        topics (vec (range num-topics))
        sub-chs (vec (for [t topics]
                       (let [ch (csp/channel 10)]
                         (channels/sub! p t ch)
                         ch)))]

    (doseq [ch sub-chs]
      (future
        (dotimes [_ per-topic]
          (csp/take! ch))))

    (dotimes [i total]
      (csp/put! source {:topic (topics (mod i num-topics)) :val i}))

    (csp/close! source)))

(defn- run-async-pubsub
  [n num-topics]
  (let [per-topic (int (/ n num-topics))
        total (* per-topic num-topics)
        source (a/chan 1000)
        p (a/pub source :topic)
        topics (vec (range num-topics))
        sub-chs (vec (for [t topics]
                       (let [ch (a/chan 10)]
                         (a/sub p t ch)
                         ch)))]

    (doseq [ch sub-chs]
      (a/thread
        (dotimes [_ per-topic]
          (a/<!! ch))))

    (dotimes [i total]
      (a/>!! source {:topic (topics (mod i num-topics)) :val i}))

    (a/close! source)))

(deftest ^:performance pubsub-performance-tests
  (testing "Pub/Sub Topic Routing (1 source, 5 topics, 1 subscriber each, 100k items)"
    (println "\n--- BENCHMARKING: csp-clj pub/sub ---")
    (criterium/quick-bench (run-csp-pubsub iterations 5))

    (println "\n--- BENCHMARKING: core.async pub/sub ---")
    (criterium/quick-bench (run-async-pubsub iterations 5))

    (is true "Benchmarks completed successfully")))

(deftest ^:performance pubsub-scaling-performance-tests
  (testing "Pub/Sub Scaling (varying topic count, 10k items per config)"
    ;; Reduced iterations (10k per config) to keep total runtime bounded
    ;; across 3 configurations x 2 libraries x criterium's multiple evals.
    (let [scaling-iterations 10000]
      (doseq [num-topics [2 5 10]]
        (println (str "\n--- BENCHMARKING (" num-topics " topics): csp-clj pub/sub ---"))
        (criterium/quick-bench (run-csp-pubsub scaling-iterations num-topics))
        (println (str "\n--- BENCHMARKING (" num-topics " topics): core.async pub/sub ---"))
        (criterium/quick-bench (run-async-pubsub scaling-iterations num-topics))))
    (is true "Benchmarks completed successfully")))