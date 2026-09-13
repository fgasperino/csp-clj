(ns csp-clj.channels.multiplexer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.channels :as channels]
   [csp-clj.protocols.channel :as channel-protocol]))

(deftest ^:unit multiplex-tests

  (testing "multiplex behavior"

    (testing "=> basic multiplexing"

      (let [source (channels/create)
            m (channels/multiplex source)
            tap1 (channels/create 10)
            tap2 (channels/create 10)]

        (channels/tap! m tap1)
        (channels/tap! m tap2)

        (testing "==> values are delivered to all taps"

          (channels/put! source :hello)

          (is (= :hello (channels/take! tap1 100)) "===> tap1 received")
          (is (= :hello (channels/take! tap2 100)) "===> tap2 received")

          (channels/put! source :world)

          (is (= :world (channels/take! tap1 100)) "===> tap1 received second")
          (is (= :world (channels/take! tap2 100)) "===> tap2 received second"))))

    (testing "=> strict backpressure: mult waits for all taps before the next value"

      ;; Dispatch is sequential, so which tap is served first is unspecified.
      ;; Verify the backpressure guarantee order-agnostically: the mult must
      ;; not take value N+1 from the source until every tap has accepted N.
      (let [taken (atom 0)
            real-source (channels/create 10)
            counting-source (reify channel-protocol/Channel
                              (take! [_]
                                (let [v (channel-protocol/take! real-source)]
                                  (when (some? v)
                                    (swap! taken inc))
                                  v))
                              (take! [_ t]
                                (channel-protocol/take! real-source t))
                              (put! [_ v]
                                (channel-protocol/put! real-source v))
                              (put! [_ v t]
                                (channel-protocol/put! real-source v t))
                              (close! [_]
                                (channel-protocol/close! real-source))
                              (closed? [_]
                                (channel-protocol/closed? real-source)))
            m (channels/multiplex counting-source)
            tap1 (channels/create)
            tap2 (channels/create)]

        (channels/tap! m tap1)
        (channels/tap! m tap2)

        ;; Two values are available in the buffered source
        (channel-protocol/put! real-source 1)
        (channel-protocol/put! real-source 2)

        (Thread/sleep 50)

        (is (= 1 @taken)
            "===> only value 1 is taken while the first tap is blocked")

        ;; Consume from both taps concurrently (order-agnostic)
        (let [f1 (future (channels/take! tap1 1000))
              f2 (future (channels/take! tap2 1000))]
          (is (= 1 @f1) "===> tap1 gets value 1")
          (is (= 1 @f2) "===> tap2 gets value 1"))

        (Thread/sleep 50)

        (is (= 2 @taken)
            "===> value 2 is taken only after both taps accepted value 1")

        ;; Drain value 2, then close so the dispatcher exits
        (let [f1 (future (channels/take! tap1 1000))
              f2 (future (channels/take! tap2 1000))]
          (is (= 2 @f1) "===> tap1 gets value 2")
          (is (= 2 @f2) "===> tap2 gets value 2"))

        (channels/close! real-source)))

    (testing "=> closing semantics"

      (testing "==> closes taps when close? is true (default)"

        (let [source (channels/create)
              m (channels/multiplex source)
              tap1 (channels/create 10)]

          (channels/tap! m tap1)
          (channels/put! source :val)

          (is (= :val (channels/take! tap1 100)))

          (channels/close! source)
          (Thread/sleep 50)

          (is (channels/closed? tap1) "===> tap1 was closed by source close")))

      (testing "==> does not close taps when close? is false"

        (let [source (channels/create)
              m (channels/multiplex source)
              tap1 (channels/create 10)]

          (channels/tap! m tap1 false)
          (channels/close! source)
          (Thread/sleep 50)

          (is (not (channels/closed? tap1)) "===> tap1 left open"))))

    (testing "=> tap management"

      (let [source (channels/create)
            m (channels/multiplex source)
            tap1 (channels/create 10)
            tap2 (channels/create 10)]

        (channels/tap! m tap1)
        (channels/tap! m tap2)

        (testing "==> untap removes a tap"

          (channels/untap! m tap1)
          (channels/put! source :hello)

          (is (= :timeout (channels/take! tap1 100)) "===> tap1 no longer receives")
          (is (= :hello (channels/take! tap2 100)) "===> tap2 still receives"))

        (testing "==> untap-all removes all taps"

          (channels/untap-all! m)
          (channels/put! source :world)

          (is (= :timeout (channels/take! tap2 100)) "===> tap2 no longer receives"))))

    (testing "=> auto-cleanup: closed taps removed automatically (try-commit! returns false)"

      (let [source (channels/create)
            m (channels/multiplex source)
            tap1 (channels/create 10)]

        (channels/tap! m tap1)
        (channels/close! tap1)
        (channels/put! source :hello)

        (Thread/sleep 50)

        (let [tap2 (channels/create 10)]

          (channels/tap! m tap2)
          (channels/put! source :world)

          (is (= :world (channels/take! tap2 100)) "===> mult continues after removing closed tap"))))

    (testing "=> multiplex error handling"

      (testing "==> throwing tap is removed and other taps continue"

        (testing "===> tap that throws on put! is silently removed"

          (let [source (channels/create)
                m (channels/multiplex source)
                good-tap (channels/create 10)
                  ;; A tap channel whose put! throws after the first message
                call-count (atom 0)
                throwing-tap (reify channel-protocol/Channel
                               (put! [_ _]
                                 (let [n (swap! call-count inc)]
                                   (if (> n 1)
                                     (throw (RuntimeException. "tap exploded"))
                                     true)))
                               (put! [_ _ _] true)
                               (take! [_] nil)
                               (take! [_ _] nil)
                               (close! [_] nil)
                               (closed? [_] false))]

            (channels/tap! m good-tap)
            (channels/tap! m throwing-tap)

              ;; First message: both taps succeed
            (channels/put! source :first)
            (is (= :first (channels/take! good-tap 200))
                "====> good-tap receives first message")

              ;; Second message: throwing-tap throws, should be removed
            (channels/put! source :second)
            (is (= :second (channels/take! good-tap 200))
                "====> good-tap receives second message despite throwing tap")

              ;; Third message: only good-tap should receive (throwing-tap was removed)
            (channels/put! source :third)
            (is (= :third (channels/take! good-tap 200))
                "====> good-tap receives third message, mult still functioning")

            (channels/close! source)))))))

(deftest ^:functional multiplex-toctou-tests

  (testing "multiplexer late tap resource leaks"

    (testing "=> tapping a closed multiplexer"

      (testing "==> immediately closes the tap channel"

        (let [source (channels/create)
              m (channels/multiplex source)
              late-tap (channels/create 5)]

          ;; Close source and allow dispatch thread to terminate
          (channels/close! source)
          (Thread/sleep 50)

          ;; Tap the now-closed mult. Because mult is closed, it should immediately
          ;; close the provided tap channel without holding onto it.
          (channels/tap! m late-tap true)

          (is (true? (channels/closed? late-tap))
              "===> late tap is immediately closed")
          (is (nil? (channels/take! late-tap 100))
              "===> late tap yields EOF")
          (is (= 0 (.size ^java.util.concurrent.ConcurrentHashMap (:taps m)))
              "===> mult does not hold reference to late tap"))))))

(deftest ^:unit multiplex-error-handling-tests

  (testing "dispatch-loop error runs ex-handler and cleans up"

    (testing "=> source take! throwing triggers ex-handler, cleanup, and tap close"
      (let [call-count (atom 0)
            real-source (channels/create 10)
            ;; Source that yields a value on the first take! and then throws,
            ;; forcing the dispatch-loop's outer catch without relying on the
            ;; internal executor (an implementation detail).
            throwing-source (reify channel-protocol/Channel
                              (take! [_]
                                (let [n (swap! call-count inc)]
                                  (if (> n 1)
                                    (throw (RuntimeException. "source exploded"))
                                    (channel-protocol/take! real-source))))
                              (take! [_ timeout-ms]
                                (channel-protocol/take! real-source timeout-ms))
                              (put! [_ v]
                                (channel-protocol/put! real-source v))
                              (put! [_ v t]
                                (channel-protocol/put! real-source v t))
                              (close! [_]
                                (channel-protocol/close! real-source))
                              (closed? [_]
                                (channel-protocol/closed? real-source)))
            handler-called (atom false)
            m (channels/multiplex throwing-source
                                  {:ex-handler (fn [_]
                                                 (reset! handler-called true)
                                                 ;; Throwing here must not skip cleanup
                                                 (throw (RuntimeException. "ex-handler boom")))})
            tap-ch (channels/create 10)]

        ;; Seed a value so the first take! returns and the second throws
        (channel-protocol/put! real-source :first)
        (channels/tap! m tap-ch)

        (Thread/sleep 100)

        (is @handler-called "===> ex-handler was called")
        (is (channels/closed? tap-ch) "===> close?=true tap closed during cleanup")
        (is (= 0 (.size ^java.util.concurrent.ConcurrentHashMap (:taps m)))
            "===> multiplexer cleaned up taps")

        ;; After cleanup, tapping should immediately close the channel
        (let [late-tap (channels/create)]
          (channels/tap! m late-tap true)
          (is (channels/closed? late-tap)
              "===> late tap closed (cleanup ran despite ex-handler throwing)")
          (is (= 0 (.size ^java.util.concurrent.ConcurrentHashMap (:taps m)))
              "===> multiplexer holds no late tap"))))))

(deftest ^:functional multiplex-sequential-dispatch-tests

  (testing "sequential dispatch"

    (testing "=> all unbuffered taps receive every value in order"

      ;; Consumers start first so the sequentially-dispatched puts can be
      ;; accepted in whatever order the tap snapshot happens to iterate.
      (let [source (channels/create 50)
            m (channels/multiplex source)
            taps (vec (repeatedly 3 channels/create))]

        (doseq [t taps]
          (channels/tap! m t))

        (let [results (mapv (fn [t]
                              (future
                                (vec (repeatedly 5 #(channels/take! t 1000)))))
                            taps)]
          (doseq [i (range 5)]
            (channels/put! source i))
          (channels/close! source)
          (doseq [r results]
            (is (= [0 1 2 3 4] @r)
                "===> tap received all values in order")))))

    (testing "=> untap mid-stream stops delivery without hanging"

      (let [source (channels/create)
            m (channels/multiplex source)
            t1 (channels/create 10)
            t2 (channels/create 10)]

        (channels/tap! m t1)
        (channels/tap! m t2)

        (channels/untap! m t1)
        (channels/put! source :only-t2)

        (is (= :timeout (channels/take! t1 100)) "===> t1 no longer receives")
        (is (= :only-t2 (channels/take! t2 200)) "===> t2 still receives")

        (channels/close! source)))

    (testing "=> per-tap order preserved with many taps"

      (let [source (channels/create 50)
            m (channels/multiplex source)
            taps (vec (repeatedly 20 #(channels/create 50)))]

        (doseq [t taps]
          (channels/tap! m t))

        (doseq [i (range 10)]
          (channels/put! source i))
        (channels/close! source)

        (doseq [t taps]
          (is (= (range 10)
                 (vec (repeatedly 10 #(channels/take! t 500))))
              "===> each tap receives all values in order"))))))
