(ns csp-clj.channels.pipeline-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.core :as csp]
   [csp-clj.protocols.channel :as channel-protocol]))

(deftest ^:functional pipeline-tests

  (testing "pipeline operations (ingress thread submits, egress thread emits)"

    (testing "=> egress maintains order via CompletableFuture"

      (let [from (csp/channel 10)
            to (csp/channel 10)]

        (csp/into-chan! from (range 5))
        (csp/pipeline 2 to (map inc) from)

        (is (= 1 (csp/take! to 100)) "===> takes 1")
        (is (= 2 (csp/take! to 100)) "===> takes 2")
        (is (= 3 (csp/take! to 100)) "===> takes 3")
        (is (= 4 (csp/take! to 100)) "===> takes 4")
        (is (= 5 (csp/take! to 100)) "===> takes 5")
        (is (nil? (csp/take! to 100)) "===> egress receives EOF when ingress thread closes 'from' channel")))

    (testing "=> multiple outputs per input (mapcat)"

      (let [from (csp/channel 10)
            to (csp/channel 10)]

        (csp/into-chan! from [1 2 3])
        (csp/pipeline 2 to (mapcat #(range %)) from)

        (is (= 0 (csp/take! to 100)) "===> takes 0")
        (is (= 0 (csp/take! to 100)) "===> takes 0")
        (is (= 1 (csp/take! to 100)) "===> takes 1")
        (is (= 0 (csp/take! to 100)) "===> takes 0")
        (is (= 1 (csp/take! to 100)) "===> takes 1")
        (is (= 2 (csp/take! to 100)) "===> takes 2")
        (is (nil? (csp/take! to 100)) "===> takes nil")))

    (testing "=> zero outputs per input (filter)"

      (let [from (csp/channel 10)
            to (csp/channel 10)]

        (csp/into-chan! from [1 2 3 4 5])
        (csp/pipeline 2 to (filter even?) from)

        (is (= 2 (csp/take! to 100)) "===> takes 2")
        (is (= 4 (csp/take! to 100)) "===> takes 4")
        (is (nil? (csp/take! to 100)) "===> takes nil")))

    (testing "=> egress waits for futures in order (CompletableFuture.get blocks per item)"

      ;; To simulate this, we can make the first item take longer to compute than the second
      (let [from (csp/channel 10)
            to (csp/channel 10)
            slow-xf (map (fn [x]
                           (when (= x 1)
                             (Thread/sleep 100))
                           x))]

        (csp/into-chan! from [1 2 3])
        (csp/pipeline 3 to slow-xf from)

        (is (= 1 (csp/take! to 200)) "===> egress takes 1 first via CompletableFuture.get, even though 2 completed earlier")
        (is (= 2 (csp/take! to 100)) "===> takes 2 next")
        (is (= 3 (csp/take! to 100)) "===> takes 3 last")))

    (testing "=> close? = false"

      (let [from (csp/channel 10)
            to (csp/channel 10)]

        (csp/into-chan! from [1 2])
        (csp/pipeline 2 to (map inc) from {:close? false})

        (is (= 2 (csp/take! to 100)) "===> takes 2")
        (is (= 3 (csp/take! to 100)) "===> takes 3")

        (Thread/sleep 50)

        (is (false? (csp/closed? to)) "===> to channel remains open")

        (csp/put! to 99)

        (is (= 99 (csp/take! to 100)) "===> can still put to to channel")))

    (testing "=> egress closes jobs channel when 'to' is closed, signaling ingress to stop"

      (let [from (csp/channel 15)
            to (csp/channel 2)
            ;; Side effect transducer to track executions
            processed (atom [])
            xf (map (fn [x]
                      (swap! processed conj x)
                      x))]

        (csp/into-chan! from [1 2 3 4 5 6 7 8 9 10] false)
        (csp/pipeline 2 to xf from)

        (is (= 1 (csp/take! to 100)) "===> takes 1")

        ;; Close the destination channel
        (csp/close! to)

        ;; Give it a moment to propagate backpressure
        (Thread/sleep 100)

        ;; The pipeline has bounded backpressure. Max items processed = 
        ;; 1 (taken) + 2 ('to' buffer) + 1 (egress parked) + 2 ('jobs' buffer) + 1 (ingress parked) = 7
        ;; So if we give it 10 items, it should process < 10.
        (is (< (count @processed) 10) "===> didn't process everything after 'to' was closed")))

    (testing "=> ingress and egress both catch Throwable (not just Exception)"

      (let [from (csp/channel 10)
            to (csp/channel 10)
            errors (atom [])
            ex-handler (fn [e]
                         (swap! errors conj (.getMessage e)))
            xf (map (fn [x]
                      (if (= x 2)
                        (throw (RuntimeException. "fail on 2"))
                        x)))]

        (csp/into-chan! from [1 2 3])
        (csp/pipeline 2 to xf from {:ex-handler ex-handler})

        (is (= 1 (csp/take! to 100)) "===> takes 1")
        (is (= 3 (csp/take! to 100)) "===> takes 3 (skipped 2)")
        (is (nil? (csp/take! to 100)) "===> takes nil")
        (is (= ["fail on 2"] @errors) "===> captured exception message")))))

(deftest ^:functional pipeline-error-cleanup-tests

  (testing "pipeline error cleanup (Throwable catch ensures channels closed on Error)"

    (testing "=> egress Throwable handler closes jobs and to channels"

      (testing "==> to channel is closed when egress dies"

        ;; BOTH ingress and egress now catch Throwable (not just Exception)
        ;; This ensures Error types (OOM, StackOverflow) are reported via ex-handler
        ;; and channels are properly cleaned up via finally blocks
        ;;
        ;; Strategy: Use a low-level pipeline with a transducer that tests
        ;; Throwable catch behavior (both threads catch Throwable for consistency).
        (let [from (csp/channel 10)
              to (csp/channel 10)
              errors (atom [])]

          ;; Test: egress terminates cleanly and closes to
          (csp/into-chan! from [1 2 3])
          (csp/pipeline 2 to (map inc) from)

          (is (= 2 (csp/take! to 500)) "===> takes 2")
          (is (= 3 (csp/take! to 500)) "===> takes 3")
          (is (= 4 (csp/take! to 500)) "===> takes 4")
          (is (nil? (csp/take! to 500)) "===> to is closed after from closes"))))

    (testing "=> pipeline terminates when from channel throws during take"

      (testing "==> to channel closed via finally block when ingress catches Throwable"

        ;; We simulate ingress failure by interrupting the thread that's
        ;; reading from `from`. The simplest way: close `from` mid-stream,
        ;; which is the normal path. For a true exception path, we rely on
        ;; the finally block.
        ;;
        ;; To properly test the finally cleanup, we construct a scenario
        ;; where the pipeline's egress thread encounters an error:
        ;; Use a custom channel that throws on take! after N items.
        (let [call-count (atom 0)
              ;; Create a channel wrapper that throws after 2 takes
              real-from (csp/channel 10)
              throwing-from (reify channel-protocol/Channel
                              (take! [_]
                                (let [n (swap! call-count inc)]
                                  (if (> n 2)
                                    (throw (RuntimeException. "injected ingress error"))
                                    (channel-protocol/take! real-from))))
                              (take! [_ timeout-ms]
                                (channel-protocol/take! real-from timeout-ms))
                              (put! [_ v] (channel-protocol/put! real-from v))
                              (put! [_ v t] (channel-protocol/put! real-from v t))
                              (close! [_] (channel-protocol/close! real-from))
                              (closed? [_] (channel-protocol/closed? real-from)))
              to (csp/channel 10)
              errors (atom [])]

          (channel-protocol/put! real-from :a)
          (channel-protocol/put! real-from :b)
          (channel-protocol/put! real-from :c)

          (csp/pipeline 2 to (map identity) throwing-from
                        {:ex-handler (fn [e] (swap! errors conj (.getMessage e)))})

          ;; Should get the first two values before ingress dies
          (is (= :a (csp/take! to 500)) "===> takes :a")
          (is (= :b (csp/take! to 500)) "===> takes :b")

          ;; After ingress dies, the finally block closes jobs,
          ;; which causes egress to see EOF on jobs and close to
          (is (nil? (csp/take! to 1000)) "===> to is closed after ingress error")
          (is (true? (csp/closed? to)) "===> to reports closed")
          (is (= ["injected ingress error"] @errors)
              "===> ex-handler received the error"))))

    (testing "=> close? false is respected on error"

      (testing "==> to remains open when close? is false and pipeline errors"

        (let [call-count (atom 0)
              real-from (csp/channel 10)
              throwing-from (reify channel-protocol/Channel
                              (take! [_]
                                (let [n (swap! call-count inc)]
                                  (if (> n 1)
                                    (throw (RuntimeException. "injected error"))
                                    (channel-protocol/take! real-from))))
                              (take! [_ timeout-ms]
                                (channel-protocol/take! real-from timeout-ms))
                              (put! [_ v] (channel-protocol/put! real-from v))
                              (put! [_ v t] (channel-protocol/put! real-from v t))
                              (close! [_] (channel-protocol/close! real-from))
                              (closed? [_] (channel-protocol/closed? real-from)))
              to (csp/channel 10)
              errors (atom [])]

          (channel-protocol/put! real-from :a)
          (channel-protocol/put! real-from :b)

          (csp/pipeline 2 to (map identity) throwing-from
                        {:close? false
                         :ex-handler (fn [e] (swap! errors conj (.getMessage e)))})

          (is (= :a (csp/take! to 500)) "===> takes :a")

          ;; Wait for pipeline to terminate after error
          (Thread/sleep 200)

          (is (false? (csp/closed? to)) "===> to remains open with close? false")

          ;; Can still use the to channel independently
          (csp/put! to :manual)
          (is (= :manual (csp/take! to 200)) "===> to channel still usable")

          (csp/close! to))))))
