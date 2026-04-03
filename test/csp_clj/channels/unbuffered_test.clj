(ns csp-clj.channels.unbuffered-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.channels :as channels]
   [csp-clj.channels.unbuffered :as unbuffered]
   [csp-clj.protocols.channel :as protocol])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]))

(deftest ^:unit unbuffered-channel-tests

  (testing "unbuffered channel"

    (testing "=> creation"

      (let [ch (unbuffered/create)]

        (is (satisfies? protocol/Channel ch)
            "==> satisfies the Channel protocol")
        (is (false? (channels/closed? ch))
            "==> is open when created")))

    (testing "=> synchronous rendezvous operations (phase 1 requires matching waiter)"

      (testing "==> basic rendezvous"

        (let [ch (unbuffered/create)]

          (future
            (channels/put! ch :test))

          (is (= :test (channels/take! ch))
              "===> take value from channel")))

      (testing "==> put! phase 2 blocks until matching take! arrives (rendezvous)"

        (let [ch (unbuffered/create)]

          (is (= :timeout (channels/put! ch :test 1))
              "===> phase 2: put! blocks (no taker waiting)")

          (future
            (channels/take! ch))

          (is (true? (channels/put! ch :test))
              "===> phase 1: put! finds taker via rendezvous")))

      (testing "==> take! phase 2 blocks until matching put! arrives (rendezvous)"))

    (testing "=> closed operations"

      (testing "==> close!"

        (testing "===> on open channel"

          (let [ch (unbuffered/create)]

            (is (true? (channels/close! ch))
                "====> succeeds")
            (is (true? (channels/closed? ch))
                "====> then reports as closed")))

        (testing "===> on closed channel"

          (let [ch (unbuffered/create)]

            (channels/close! ch)

            (is (true? (channels/close! ch))
                "====> close! is idempotent"))))

      (testing "==> put! on closed channel"

        (let [ch (unbuffered/create)]

          (channels/close! ch)

          (is (false? (channels/put! ch :test))
              "===> returns false")))

      (testing "==> take! on closed channel"

        (let [ch (unbuffered/create)]

          (channels/close! ch)

          (is (nil? (channels/take! ch))
              "===> returns nil"))))

    (testing "=> timeout operations"

      (testing "==> take! with timeout"

        (let [ch (unbuffered/create)]

          (is (= :timeout (channels/take! ch 1))
              "===> returns :timeout on empty channel"))))))

(deftest ^:functional unbuffered-channel-functional-tests

  (testing "unbuffered channel"

    (testing "concurrency"

      (let [ch (unbuffered/create)
            num-threads 10000
            latch (CountDownLatch. (* 2 num-threads))
            received (java.util.concurrent.atomic.AtomicInteger. 0)]

        (dotimes [_ num-threads]
          (future
            (channels/put! ch 1)
            (.countDown latch)))

        (dotimes [_ num-threads]
          (future
            (when (channels/take! ch)
              (.incrementAndGet received))
            (.countDown latch)))

        (is (.await latch 30 TimeUnit/SECONDS) "===> completed in time")
        (is (= num-threads (.get received)) "===> all values received")))))
