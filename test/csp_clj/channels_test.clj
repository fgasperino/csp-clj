(ns csp-clj.channels-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.channels :as channels])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]))

(deftest edge-case-tests

  (testing "known edge cases"

    (testing "=> waiter cleanup on timeout"

      (testing "==> take! timeout removes waiter"

        (let [ch (channels/create)]

          (is (= :timeout (channels/take! ch 10)))
          (is (= 0 (.size ^java.util.ArrayDeque (:takes ch))))))

      (testing "==> put! timeout removes waiter"

        (let [ch (channels/create)]

          (is (= :timeout (channels/put! ch :val 10)))
          (is (= 0 (.size ^java.util.ArrayDeque (:puts ch))))))

      (testing "==> select! timeout removes waiters"

        (let [ch1 (channels/create)
              ch2 (channels/create)]

          (is (= [nil :other :timeout] (channels/select! [[ch1 :take] [ch2 :put :val]] {:timeout 10})))
          (is (= 0 (.size ^java.util.ArrayDeque (:takes ch1))))
          (is (= 0 (.size ^java.util.ArrayDeque (:puts ch2))))))

      (testing "==> select! success cleans up other channels"

        (let [ch1 (channels/create)
              ch2 (channels/create)
              result (atom nil)]

          (future (reset! result (channels/select! [[ch1 :take] [ch2 :take]])))
          (Thread/sleep 50)

          ;; both should have a waiter
          (is (= 1 (.size ^java.util.ArrayDeque (:takes ch1))))
          (is (= 1 (.size ^java.util.ArrayDeque (:takes ch2))))

          (channels/put! ch1 :val)
          (Thread/sleep 50)

          (is (= [ch1 :take :val] @result))
          ;; Waiter on ch2 should have been cancelled by select!
          (is (= 0 (.size ^java.util.ArrayDeque (:takes ch2)))))))

    (testing "=> nil values"

      (testing "==> unbuffered channel rejects nil"

        (let [ch (channels/create)]

          (is (thrown? IllegalArgumentException
                       (channels/put! ch nil))
              "===> throws IllegalArgumentException on nil")))

      (testing "==> buffered channel rejects nil"

        (let [ch (channels/create 5)]

          (is (thrown? IllegalArgumentException
                       (channels/put! ch nil))
              "===> throws IllegalArgumentException on nil"))))

    (testing "=> multiple close operations"

      (testing "==> idempotent close"

        (let [ch (channels/create)]

          (is (true? (channels/close! ch))
              "===> first close succeeds")
          (is (true? (channels/close! ch))
              "===> second close succeeds (idempotent)")
          (is (true? (channels/close! ch))
              "===> third close succeeds (idempotent)")
          (is (true? (channels/closed? ch))
              "===> channel remains closed"))))

    (testing "=> close wakes blocked take! immediately"

      (testing "==> unbuffered channel"

        (let [ch (channels/create)
              result (atom :not-nil)
              latch (CountDownLatch. 1)]

          (future
            (reset! result (channels/take! ch))
            (.countDown latch))

          ;; Expect this to timeout
          (.await latch 1 TimeUnit/SECONDS)

          (channels/close! ch)
          (.await latch)

          (is (nil? @result)
              "take! blocks, then wakes on channel close.")))

      (testing "==> buffered channel"

        (let [ch (channels/create 5)
              result (atom :not-nil)
              latch (CountDownLatch. 1)]

          (future
            (reset! result (channels/take! ch))
            (.countDown latch))

          ;; Expect this to timeout
          (.await latch 1 TimeUnit/SECONDS)

          (channels/close! ch)
          (.await latch)

          (is (nil? @result)
              "take! blocks, then wakes on channel close."))))

    (testing "=> EOF sentinel is not returned to user"

      (testing "==> draining a closed channel with values"

        (let [ch (channels/create 3)]

          (channels/put! ch :first)
          (channels/put! ch :second)
          (channels/close! ch)

          (is (= :first (channels/take! ch))
              "===> first value is correct")
          (is (= :second (channels/take! ch))
              "===> second value is correct")
          (is (nil? (channels/take! ch))
              "===> returns nil (EOF), not sentinel object")
          (is (nil? (channels/take! ch))
              "===> subsequent takes also return nil"))))))

(deftest sentinel-and-data-loss-tests

  (testing "sentinel correctness"

    (testing "=> boolean false values"

      (testing "==> can put and take boolean false"

        (let [ch (channels/create)]

          (future (channels/put! ch false))
          (is (false? (channels/take! ch)) "===> false transferred successfully")))

      (testing "==> buffered channel can put and take boolean false"

        (let [ch (channels/create 1)]

          (is (true? (channels/put! ch false)) "===> put! returns true on success")
          (is (false? (channels/take! ch)) "===> false transferred successfully"))))

    (testing "=> blocked put! aborts correctly"

      (testing "==> put! returns false when channel closed while blocked"

        (let [ch (channels/create)
              result (atom :pending)]

          (future (reset! result (channels/put! ch :val)))

          (Thread/sleep 50)
          (channels/close! ch)
          (Thread/sleep 50)

          (is (false? @result) "===> blocked put returns false when channel closes"))))

    (testing "=> select! leaks"

      (testing "==> select! take on closed channel does not leak EOF"

        (let [ch (channels/create)
              result (atom nil)]

          (future (reset! result (channels/select! [[ch :take]])))

          (Thread/sleep 50)
          (channels/close! ch)
          (Thread/sleep 50)

          (is (= [ch :take nil] @result) "===> returns nil instead of EOF object")))

      (testing "==> select! put on closed channel does not return true"

        (let [ch (channels/create)
              result (atom nil)]

          (future (reset! result (channels/select! [[ch :put :val]])))

          (Thread/sleep 50)
          (channels/close! ch)
          (Thread/sleep 50)

          (is (= [ch :put false] @result) "===> returns false instead of true"))))))

(deftest select!-tests

  (testing "select! operations"

    (testing "=> fast path"

      (testing "==> successful take"

        (let [ch1 (channels/create)
              ch2 (channels/create 5)]

          (channels/put! ch2 :val2)

          (is (= [ch2 :take :val2] (channels/select! [[ch1 :take] [ch2 :take]]))
              "===> immediately takes available value")))

      (testing "==> successful put"

        (let [ch1 (channels/create)
              ch2 (channels/create 5)]

          (is (= [ch2 :put true] (channels/select! [[ch1 :put :val1] [ch2 :put :val2]]))
              "===> immediately puts to available buffer")
          (is (= :val2 (channels/take! ch2))
              "===> verify put succeeded"))))

    (testing "=> closed channel behavior"

      (testing "==> immediate return"

        (let [ch1 (channels/create)
              ch2 (channels/create)]

          (channels/close! ch2)

          (is (= [ch2 :take nil] (channels/select! [[ch1 :take] [ch2 :take]]))
              "===> returns nil from closed channel immediately")
          (is (= [ch2 :put false] (channels/select! [[ch1 :take] [ch2 :put :val]]))
              "===> returns false from closed channel immediately")))

      (testing "==> all channels closed"

        (let [ch1 (channels/create)
              ch2 (channels/create)]

          (channels/close! ch1)
          (channels/close! ch2)

          (is (= [nil :other :shutdown] (channels/select! [[ch1 :take] [ch2 :take]]))
              "===> returns shutdown when all are closed"))))

    (testing "=> slow path (blocking)"

      (testing "==> successful take after block"

        (let [ch1 (channels/create)
              ch2 (channels/create)
              result (atom nil)]

          (future
            (reset! result (channels/select! [[ch1 :take] [ch2 :take]])))

          (Thread/sleep 50)
          (channels/put! ch2 :val2)
          (Thread/sleep 50)

          (is (= [ch2 :take :val2] @result)
              "===> wakes and takes when value becomes available")))

      (testing "==> successful put after block"

        (let [ch1 (channels/create)
              ch2 (channels/create)
              result (atom nil)]

          (future
            (reset! result (channels/select! [[ch1 :put :val1] [ch2 :put :val2]])))

          (Thread/sleep 50)
          (channels/take! ch1)
          (Thread/sleep 50)

          (is (= [ch1 :put true] @result)
              "===> wakes and puts when space becomes available"))))

    (testing "=> timeouts"

      (testing "==> triggers timeout"

        (let [ch1 (channels/create)
              ch2 (channels/create)]

          (is (= [nil :other :timeout] (channels/select! [[ch1 :take] [ch2 :take]] {:timeout 50}))
              "===> returns timeout vector"))))))

(deftest ^:functional select!-fairness-tests

  (testing "select! fairness"

    (testing "=> fast path distributes across channels"

      (testing "==> neither channel is overwhelmingly favored"

        ;; Both channels have a waiting producer (unbuffered, so put! blocks).
        ;; select! fast path calls try-nonblock! in shuffled order. The first
        ;; channel tried will have its putter matched. With fair shuffling,
        ;; each channel should win roughly half the time.
        (let [iterations 200
              wins (atom {:ch1 0 :ch2 0})]

          (dotimes [_ iterations]
            (let [ch1 (channels/create)
                  ch2 (channels/create)]

              ;; Put producers on both channels (they block, waiting for a taker)
              (future (channels/put! ch1 :v1))
              (future (channels/put! ch2 :v2))

              ;; Let both producers enter the puts queue
              (Thread/sleep 5)

              ;; select! fast path: try-nonblock! finds a putter on the first
              ;; channel it checks (shuffled order determines which)
              (let [[ch _ _] (channels/select! [[ch1 :take] [ch2 :take]])]
                (cond
                  (identical? ch ch1) (swap! wins update :ch1 inc)
                  (identical? ch ch2) (swap! wins update :ch2 inc)))

              ;; Drain the remaining producer
              (channels/close! ch1)
              (channels/close! ch2)))

          ;; With 200 iterations and fair shuffling, each channel should win
          ;; at least 20% of the time. A biased implementation would give
          ;; the first channel ~100% wins.
          (let [{:keys [ch1 ch2]} @wins
                total (+ ch1 ch2)]
            (is (> ch1 (* 0.2 total))
                (str "===> ch1 should win >20% of the time, got "
                     ch1 "/" total))
            (is (> ch2 (* 0.2 total))
                (str "===> ch2 should win >20% of the time, got "
                     ch2 "/" total))))))))

(deftest into-chan!-tests
  (testing "into-chan! operations"

    (testing "=> creating new channel"

      (testing "==> puts all items and closes"

        (let [ch (channels/into-chan! [1 2 3])]

          (is (= 1 (channels/take! ch)) "===> takes first item")
          (is (= 2 (channels/take! ch)) "===> takes second item")
          (is (= 3 (channels/take! ch)) "===> takes third item")
          (is (nil? (channels/take! ch)) "===> takes nil (EOF)"))))

    (testing "=> using existing channel"

      (testing "==> puts all items and closes"

        (let [ch (channels/create 5)]

          (channels/into-chan! ch [1 2 3])

          (is (= 1 (channels/take! ch)) "===> takes first item")
          (is (= 2 (channels/take! ch)) "===> takes second item")
          (is (= 3 (channels/take! ch)) "===> takes third item")
          (is (nil? (channels/take! ch)) "===> takes nil (EOF)"))))

    (testing "=> close? = false"

      (testing "==> leaves channel open"

        (let [ch (channels/create 5)]

          (channels/into-chan! ch [1 2] false)

          (is (= 1 (channels/take! ch)) "===> takes first item")
          (is (= 2 (channels/take! ch)) "===> takes second item")

          (Thread/sleep 50)

          (is (false? (channels/closed? ch)) "===> channel remains open")
          (is (= true (channels/put! ch 3)) "===> can still put to channel"))))

    (testing "=> short-circuit behavior"

      (testing "==> stops putting if channel is closed"

        (let [ch (channels/create)]

          (channels/into-chan! ch [1 2 3 4 5])

          (is (= 1 (channels/take! ch)) "===> takes first item")

          (channels/close! ch)
          (Thread/sleep 50)

          (is (nil? (channels/take! ch)) "===> takes nil (EOF)")
          (is (true? (channels/closed? ch)) "===> channel is closed"))))))

