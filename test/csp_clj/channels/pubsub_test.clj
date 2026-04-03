(ns csp-clj.channels.pubsub-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.channels :as channels])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit ConcurrentLinkedQueue]))

(deftest ^:functional pubsub-tests

  (testing "pub and sub"

    (testing "=> lazy topic creation on first sub, basic routing"

      (let [source (channels/create)
            p (channels/pub! source :type)
            a-ch (channels/create 5)
            b-ch (channels/create 5)]

        (channels/sub! p :a a-ch)
        (channels/sub! p :b b-ch)

        (channels/put! source {:type :a :val 1})
        (channels/put! source {:type :b :val 2})
        (channels/put! source {:type :a :val 3})

        (is (= {:type :a :val 1} (channels/take! a-ch 100)) "===> a-ch receives first :a message")
        (is (= {:type :a :val 3} (channels/take! a-ch 100)) "===> a-ch receives second :a message")
        (is (= :timeout (channels/take! a-ch 50)) "===> a-ch receives no more messages")

        (is (= {:type :b :val 2} (channels/take! b-ch 100)) "===> b-ch receives :b message")
        (is (= :timeout (channels/take! b-ch 50)) "===> b-ch receives no more messages")))

    (testing "=> multiplexer per topic broadcasts to multiple subscribers"

      (let [source (channels/create)
            p (channels/pub! source :type)
            a1-ch (channels/create 5)
            a2-ch (channels/create 5)]

        (channels/sub! p :a a1-ch)
        (channels/sub! p :a a2-ch)

        (channels/put! source {:type :a :val 1})

        (is (= {:type :a :val 1} (channels/take! a1-ch 100)) "===> a1-ch receives message")
        (is (= {:type :a :val 1} (channels/take! a2-ch 100)) "===> a2-ch receives message")))

    (testing "=> unsub removes subscriber (auto-cleanup if last)"

      (let [source (channels/create)
            p (channels/pub! source :type)
            a-ch (channels/create 5)]

        (channels/sub! p :a a-ch)
        (channels/put! source {:type :a :val 1})

        (is (= {:type :a :val 1} (channels/take! a-ch 100)) "===> a-ch receives message")

        (channels/unsub! p :a a-ch)
        (channels/put! source {:type :a :val 2})

        (is (= :timeout (channels/take! a-ch 50)) "===> a-ch receives no more messages after unsub")))

    (testing "=> unsub-all"

      (testing "==> unsub-all! by topic closes internal channel and removes mult"

        (let [source (channels/create)
              p (channels/pub! source :type)
              a1-ch (channels/create 5)
              a2-ch (channels/create 5)]

          (channels/sub! p :a a1-ch)
          (channels/sub! p :a a2-ch)

          (channels/unsub-all! p :a)
          (channels/put! source {:type :a :val 1})

          (is (= :timeout (channels/take! a1-ch 50)) "===> a1-ch receives no more messages after topic removed")
          (is (= :timeout (channels/take! a2-ch 50)) "===> a2-ch receives no more messages after topic removed")))

      (testing "==> unsub-all! all topics iterates snapshot, closes all internal channels"

        (let [source (channels/create)
              p (channels/pub! source :type)
              a-ch (channels/create 5)
              b-ch (channels/create 5)]

          (channels/sub! p :a a-ch)
          (channels/sub! p :b b-ch)

          (channels/unsub-all! p)
          (channels/put! source {:type :a :val 1})
          (channels/put! source {:type :b :val 2})

          (is (= :timeout (channels/take! a-ch 50)) "===> a-ch receives no more messages (topic removed)")
          (is (= :timeout (channels/take! b-ch 50)) "===> b-ch receives no more messages (topic removed)"))))

    (testing "=> source closure"

      (let [source (channels/create)
            p (channels/pub! source :type)
            a-ch (channels/create 5)]

        (channels/sub! p :a a-ch)

        ;; Put a message so that the internal mult channel for :a is created
        (channels/put! source {:type :a :val 1})

        (is (= {:type :a :val 1} (channels/take! a-ch 100)) "===> a-ch receives message")

        (channels/close! source)

        ;; Since close?=true by default for sub, a-ch should be closed when source closes
        (is (nil? (channels/take! a-ch 100)) "===> a-ch receives nil (EOF) after source closes")))))

(deftest ^:functional pubsub-concurrency-tests

  (testing "ConcurrentHashMap compute handles concurrent sub/unsub atomically"

    (testing "=> TOCTOU race: unsub last subscriber vs sub new subscriber (compute atomicity)"

      (testing "==> new subscriber receives messages after race"

        ;; This exercises the TOCTOU race: unsub! removing the last subscriber
        ;; while sub! adds a new one on the same topic. The new subscriber must
        ;; end up on a live multiplexer.
        (dotimes [_ 50]
          (let [source (channels/create)
                p (channels/pub! source :type)
                initial-ch (channels/create 5)
                race-ch (channels/create 5)
                start-latch (CountDownLatch. 1)
                done-latch (CountDownLatch. 2)]

            ;; Subscribe initial channel so unsub! has something to remove
            (channels/sub! p :a initial-ch)

            ;; Warm up: ensure the internal mult for :a is created
            (channels/put! source {:type :a :val :warmup})
            (channels/take! initial-ch 200)

            ;; Race: unsub! last subscriber vs sub! new subscriber
            (future
              (.await start-latch)
              (channels/unsub! p :a initial-ch)
              (.countDown done-latch))

            (future
              (.await start-latch)
              (channels/sub! p :a race-ch)
              (.countDown done-latch))

            (.countDown start-latch)
            (.await done-latch 5 TimeUnit/SECONDS)

            ;; Allow dispatch loop to settle
            (Thread/sleep 10)

            ;; Send a message. The new subscriber should receive it
            ;; because it's on a live mult (either the original or a new one).
            (channels/put! source {:type :a :val :after-race})
            (let [result (channels/take! race-ch 200)]
              (is (= {:type :a :val :after-race} result)
                  "===> new subscriber receives messages after concurrent unsub/sub"))

            (channels/close! source)))))

    (testing "=> ConcurrentHashMap compute stress test (no lost updates under contention)"

      (testing "==> no exceptions under heavy contention"

        (let [source (channels/create 100)
              p (channels/pub! source :type)
              num-iterations 200
              errors (ConcurrentLinkedQueue.)
              done-latch (CountDownLatch. (* 2 num-iterations))]

          ;; Half threads subscribe, half unsubscribe, all on the same topic
          (dotimes [i num-iterations]
            (future
              (try
                (let [ch (channels/create 5)]
                  (channels/sub! p :stress ch)
                  ;; Brief pause to let the sub take effect
                  (Thread/sleep 1)
                  (channels/unsub! p :stress ch))
                (catch Exception e
                  (.add errors e))
                (finally
                  (.countDown done-latch))))

            (future
              (try
                (let [ch (channels/create 5)]
                  (channels/sub! p :stress ch))
                (catch Exception e
                  (.add errors e))
                (finally
                  (.countDown done-latch)))))

          (.await done-latch 30 TimeUnit/SECONDS)

          (is (zero? (.size errors))
              (str "===> no exceptions thrown, got: "
                   (when-let [e (.peek errors)]
                     (.getMessage ^Exception e))))

          (channels/close! source))))

    (testing "=> lazy topic recreation after unsub-all! removes topic"

      (testing "==> new subscriber works after unsub-all"

        (let [source (channels/create)
              p (channels/pub! source :type)
              ch1 (channels/create 5)]

          ;; Subscribe, verify working
          (channels/sub! p :a ch1)
          (channels/put! source {:type :a :val 1})

          (is (= {:type :a :val 1} (channels/take! ch1 200))
              "===> ch1 receives before unsub-all")

          ;; Nuke everything
          (channels/unsub-all! p)

          ;; Re-subscribe on same topic with new channel
          (let [ch2 (channels/create 5)]
            (channels/sub! p :a ch2)
            (channels/put! source {:type :a :val 2})

            (is (= {:type :a :val 2} (channels/take! ch2 200))
                "===> ch2 receives after unsub-all + re-sub"))

          (channels/close! source))))))

(deftest ^:functional pubsub-toctou-tests

  (testing "pubsub late sub resource leaks"

    (testing "=> subscribing to a closed publisher"

      (testing "==> immediately closes the subscriber channel"

        (let [source (channels/create)
              p (channels/pub! source :type)
              late-sub (channels/create 5)]
          ;; Close source and allow dispatch thread to terminate

          (channels/close! source)
          (Thread/sleep 50)

          ;; Sub to the now-closed pub. Because pub is closed, it should immediately
          ;; close the provided sub channel without holding onto it or making a new mult.
          (channels/sub! p :a late-sub true)

          (is (true? (channels/closed? late-sub))
              "===> late sub is immediately closed")
          (is (nil? (channels/take! late-sub 100))
              "===> late sub yields EOF")
          (is (empty? (.keySet ^java.util.concurrent.ConcurrentHashMap (:topic-mults p)))
              "===> pub does not create new mult for late sub"))))))
