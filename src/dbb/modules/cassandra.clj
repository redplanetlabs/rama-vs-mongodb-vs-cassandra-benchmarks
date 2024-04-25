(ns dbb.modules.cassandra
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops])
  (:import [java.util.concurrent Semaphore]))

(defmodule CassandraModule [setup topologies]
  (declare-depot setup *insert-depot :random)

  (let [s (stream-topology topologies "cassandra")]
    (declare-pstate s $$primary {java.util.List String})
    (<<sources s
      (source> *insert-depot :> *data)
      (ops/explode *data :> [*pk *ck *val])
      (|hash *pk)
      (local-transform> [(keypath [*pk *ck]) (termval *val)] $$primary)
      )))

(defn gen-uuid []
  (str (random-uuid)))


(defn gen-batch [batch-size]
  (loop [i 0
         res []]
    (if (= i batch-size)
      res
      (recur (inc i) (conj res [(gen-uuid) (gen-uuid) (gen-uuid)])))))

(defn run-batch-insert-load [conductor-host batch-size]
  (let [manager (open-cluster-manager-internal {"conductor.host" conductor-host})
        depot (foreign-depot manager (get-module-name CassandraModule) "*insert-depot")
        sem (Semaphore. 10000)
        start-time (System/currentTimeMillis)
        target (long (/ 100000 batch-size))
        added (atom 0)]
    (loop [counter 0]
      (.acquire sem batch-size)
      (when (= 0 (mod counter target))
        (println "INSERTED" @added "in" (- (System/currentTimeMillis) start-time) "millis"))
      (.whenComplete
        (foreign-append-async! depot (gen-batch batch-size) :ack)
        (reify java.util.function.BiConsumer
          (accept [this t u]
            (swap! added + batch-size)
            (.release sem batch-size)
            )))
      (recur (inc counter)))
    ))

(defn gen-batch-fixed [id-vol batch-size]
  (loop [i 0
         res []]
    (let [id (str (vswap! id-vol + 2))]
      (if (= i batch-size)
        res
        (recur (inc i) (conj res [id id id]))))))

(defn run-batch-insert-load-fixed [conductor-host batch-size start-id end-id]
  (let [manager (open-cluster-manager-internal {"conductor.host" conductor-host})
        depot (foreign-depot manager (get-module-name CassandraModule) "*insert-depot")
        sem (Semaphore. 10000)
        start-time (System/currentTimeMillis)
        target (long (/ 100000 batch-size))
        added (atom 0)
        id-vol (volatile! start-id)]
    (loop [counter 0]
      (when (< @id-vol end-id)
        (.acquire sem batch-size)
        (when (= 0 (mod counter target))
          (println "INSERTED" @added "in" (- (System/currentTimeMillis) start-time) "millis"))
        (.whenComplete
          (foreign-append-async! depot (gen-batch-fixed id-vol batch-size) :ack)
          (reify java.util.function.BiConsumer
            (accept [this t u]
              (swap! added + batch-size)
              (.release sem batch-size)
              )))
        (recur (inc counter)))
      )))

(defn mixed-read-write-load [conductor-host num-pending start-id]
  (let [manager (open-cluster-manager-internal {"conductor.host" conductor-host})
        depot (foreign-depot manager (get-module-name CassandraModule) "*insert-depot")
        pstate (foreign-pstate manager (get-module-name CassandraModule) "$$primary")
        sem (Semaphore. num-pending)
        start-time (System/currentTimeMillis)
        id-vol (volatile! start-id)
        random (java.util.concurrent.ThreadLocalRandom/current)]
    (loop [counter 0]
      (.acquire sem 1)
      (when (= 0 (mod counter 100000))
        (println "FINISHED" counter "in" (- (System/currentTimeMillis) start-time) "millis"))
      (let [counter (atom 0)
            ;; do miss 50% of the time
            query-id (str (.nextLong random @id-vol))
            finished! (fn [] (when (= 2 (swap! counter inc)) (.release sem 1)))
            insert-id (str (vswap! id-vol + 2))]
        (.whenComplete
          (foreign-append-async! depot [[insert-id insert-id insert-id]] :ack)
          (reify java.util.function.BiConsumer
            (accept [this t u]
              (when (some? u) (println "APPEND ERROR" u))
              (finished!)
              )))
        (.whenComplete
          (foreign-select-one-async (keypath [query-id query-id]) pstate)
          (reify java.util.function.BiConsumer
            (accept [this t u]
              (when (some? u) (println "QUERY ERROR" u))
              (finished!)
              ))))
      (recur (inc counter)))
    ))
