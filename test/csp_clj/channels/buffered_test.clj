(ns csp-clj.channels.buffered-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.buffers :as buffers]
   [csp-clj.channels :as channels]
   [csp-clj.channels.buffered :as buffered]
   [csp-clj.protocols.channel :as protocol])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]))

(deftest ^:unit buffered-channel-tests

  (testing "buffered channel"

    (testing "=> creation"

      (testing "==> with capacity"

        (let [ch (buffered/create 5)]

          (is (satisfies? protocol/Channel ch)
              "===> satisfies the Channel protocol")
          (is (false? (channels/closed? ch))
              "===> is open when created")))

      (testing "==> with buffer"

        (let [ch (buffered/create (buffers/fixed-buffer 5))]

          (is (satisfies? protocol/Channel ch)
              "===> satisfies the Channel protocol")
          (is (false? (channels/closed? ch))
              "===> is open when created"))))

    (testing "=> put! phase 1 completes immediately when buffer has capacity"

      (let [ch (buffered/create 5)]

        (is (true? (channels/put! ch :test-1))
            "==> phase 1: writes first message (buffer has space)")
        (is (true? (channels/put! ch :test-2))
            "==> phase 1: writes second message (buffer has space)")))

    (testing "=> take! phase 1 completes immediately when buffer has messages"

      (let [ch (buffered/create 5)]

        (channels/put! ch :test-1)
        (channels/put! ch :test-2)

        (is (= :test-1 (channels/take! ch))
            "==> phase 1: reads first message (buffer non-empty)")
        (is (= :test-2 (channels/take! ch))
            "==> phase 1: reads second message (buffer non-empty)")))

    (testing "=> put! phase 2 blocks when buffer full (waits for take! to create space)"

      (let [ch (buffered/create 1)]

        (is (true? (channels/put! ch :test-1))
            "==> phase 1: writes first message (buffer has space)")
        (is (= :timeout (channels/put! ch :test-2 1))
            "==> phase 2: blocks on second message (buffer full, no taker)")))

    (testing "=> take! phase 2 blocks when buffer empty (waits for put! to add message)"

      (let [ch (buffered/create 1)]

        (channels/put! ch :test-1)

        (is (= :test-1 (channels/take! ch))
            "==> phase 1: reads first message (buffer non-empty)")
        (is (= :timeout (channels/take! ch 1))
            "==> phase 2: blocks on second message (buffer empty)")))

    (testing "=> fast-path closed check outside lock (phase 1 fails fast)"

      (testing "==> close!"

        (testing "===> on open channel"

          (let [ch (buffered/create 5)]

            (is (true? (channels/close! ch))
                "====> phase 1: closed check passes, sets closed flag")
            (is (true? (channels/closed? ch))
                "====> fast path: subsequent closed? checks return true")))

        (testing "===> on closed channel"

          (let [ch (buffered/create 5)]

            (channels/close! ch)

            (is (true? (channels/close! ch))
                "====> fast path: closed check outside lock returns true immediately (idempotent)"))))

      (testing "==> put! on closed channel"

        (let [ch (buffered/create 5)]

          (channels/close! ch)

          (is (false? (channels/put! ch :test))
              "===> returns false")))

      (testing "==> take! on closed channel"

        (let [ch (buffered/create 5)]

          (channels/close! ch)

          (is (nil? (channels/take! ch))
              "===> returns nil")))

      (testing "==> draining a closed channel"

        (let [ch (buffered/create 5)]

          (channels/put! ch :first)
          (channels/put! ch :second)

          (channels/close! ch)

          (is (= :first (channels/take! ch))
              "===> takes first message (closed)")
          (is (= :second (channels/take! ch))
              "===> takes secon message (closed)")
          (is (nil? (channels/take! ch))
              "===> returns nil"))))

    (testing "=> fifo ordering"

      (testing "==> maintains insertion order"

        (let [ch (buffered/create 5)]

          (channels/put! ch :first)
          (channels/put! ch :second)
          (channels/put! ch :third)

          (is (= :first (channels/take! ch))
              "===> first value out is first value in")
          (is (= :second (channels/take! ch))
              "===> second value out is second value in")
          (is (= :third (channels/take! ch))
              "===> third value out is third value in"))))

    (testing "=> zero capacity handling"

      (testing "==> capacity 0 becomes capacity 1"

        (let [ch (buffered/create 0)]

          (future (channels/put! ch :value))

          (is (= :value (channels/take! ch))
              "===> can put and take with zero capacity"))))

    (testing "=> timeout operations"

      (testing "==> put! with timeout on full buffer"

        (let [ch (buffered/create 1)]
          (channels/put! ch :full)

          (is (= :timeout (channels/put! ch :blocking 1))
              "===> returns :timeout when buffer full")))

      (testing "==> take! with timeout on empty buffer"

        (let [ch (buffered/create 5)]

          (is (= :timeout (channels/take! ch 1))
              "===> returns :timeout on empty buffer"))))))

(deftest ^:functional buffered-channel-functional-tests

  (testing "=> concurrency"

    (testing "==> multiple producers single consumer"

      (let [ch (buffered/create 10)
            num-producers 3
            messages-per-producer 5
            total-messages (* num-producers messages-per-producer)
            received (atom [])]

        (doseq [p (range num-producers)]
          (future
            (doseq [m (range messages-per-producer)]
              (channels/put! ch [p m]))))

        (loop [count 0]
          (when (< count total-messages)
            (let [msg (channels/take! ch 1000)]
              (when (and msg (not= msg :timeout))
                (swap! received conj msg)
                (recur (inc count))))))

        (is (= total-messages (count @received))
            "===> all messages received")))

    (testing "==> single producer multiple consumers"

      (let [ch (buffered/create 10)
            num-consumers 3
            total-messages 15
            consumed (atom 0)
            latch (CountDownLatch. total-messages)]

        (doseq [_ (range num-consumers)]
          (future
            (loop []
              (let [msg (channels/take! ch)]
                (when msg
                  (swap! consumed inc)
                  (.countDown latch)
                  (recur))))))

        (dotimes [n total-messages]
          (channels/put! ch n))

        (channels/close! ch)
        (.await latch 5 TimeUnit/SECONDS)

        (is (= total-messages @consumed)
            "==> all messages consumed by producers")))

    (testing "==> bulk operations"

      (let [ch (buffered/create 100)
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
        (is (= num-threads (.get received)) "===> all values received"))))

  (testing "=> simple pipeline"

    (let [ch1 (buffered/create 5)
          ch2 (buffered/create 5)
          result (atom nil)
          latch (CountDownLatch. 1)]

      (future
        (channels/put! ch1 1)
        (channels/put! ch1 2)
        (channels/put! ch1 3)
        (channels/close! ch1))

      (future
        (loop []
          (let [val (channels/take! ch1)]
            (when val
              (channels/put! ch2 (* 2 val))
              (recur))))
        (channels/close! ch2))

      (future
        (let [vals (loop [acc []]
                     (let [val (channels/take! ch2)]
                       (if val
                         (recur (conj acc val))
                         acc)))]
          (reset! result vals)
          (.countDown latch)))

      (is (.await latch 5 TimeUnit/SECONDS)
          "===> pipeline completed in time")
      (is (= [2 4 6] @result)
          "===> pipeline transforms values correctly"))))
