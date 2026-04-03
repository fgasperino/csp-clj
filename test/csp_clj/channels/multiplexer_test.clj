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

    (testing "=> Phaser backpressure: mult waits for all taps via arriveAndAwaitAdvance"

      (let [source (channels/create)
            m (channels/multiplex source)
            ;; Unbuffered channels mean they block until someone takes
            tap1 (channels/create)
            tap2 (channels/create)]

        (channels/tap! m tap1)
        (channels/tap! m tap2)

        (testing "==> Phaser arriveAndAwaitAdvance blocks until all tap futures complete"

          (future
            (channels/put! source 1)
            (channels/put! source 2))

          (Thread/sleep 50)

          (is (= 1 (channels/take! tap1 100)) "===> tap1 gets 1")
          (is (= :timeout (channels/take! tap1 100)) "===> tap1 doesn't get 2 yet")
          (is (= 1 (channels/take! tap2 100)) "===> tap2 gets 1")
          (is (= 2 (channels/take! tap1 100)) "===> tap1 gets 2")
          (is (= 2 (channels/take! tap2 100)) "===> tap2 gets 2"))))

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
