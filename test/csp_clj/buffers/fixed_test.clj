(ns csp-clj.buffers.fixed-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [csp-clj.protocols.buffer :as protocol]
   [csp-clj.buffers.fixed :as fixed]))

(deftest ^:unit fixed-buffer-tests

  (testing "fixed buffer"

    (testing "=> creation"

      (testing "==> creates buffer with specified capacity"

        (let [buf (fixed/create 5)]

          (is (= 5 (protocol/capacity buf)) "===> reports correct capacity")
          (is (= 0 (protocol/size buf)) "===> initially empty")
          (is (false? (protocol/full? buf)) "===> initially not full")))

      (testing "==> enforces minimum capacity of 1"

        (let [buf (fixed/create 0)]

          (is (= 1 (protocol/capacity buf)) "===> capacity 0 becomes 1"))

        (let [buf (fixed/create -5)]

          (is (= 1 (protocol/capacity buf)) "===> negative capacity becomes 1"))))

    (testing "=> fifo behavior"

      (testing "==> adds and removes in fifo order"

        (let [buf (fixed/create 3)]

          (protocol/add! buf :first)
          (protocol/add! buf :second)
          (protocol/add! buf :third)

          (is (= 3 (protocol/size buf)) "===> count reflects three items")
          (is (true? (protocol/full? buf)) "===> buffer is now full")

          (is (= :first (protocol/remove! buf)) "===> first removed is first added")
          (is (false? (protocol/full? buf)) "===> buffer is no longer full")
          (is (= :second (protocol/remove! buf)) "===> second removed is second added")
          (is (= :third (protocol/remove! buf)) "===> third removed is third added")
          (is (= 0 (protocol/size buf)) "===> buffer is empty after removes")
          (is (nil? (protocol/remove! buf)) "===> remove from empty returns nil"))))))
