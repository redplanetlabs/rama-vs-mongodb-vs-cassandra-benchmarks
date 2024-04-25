(ns dbb.database.mongo
  (:import
    [com.mongodb.reactivestreams.client MongoClients MongoCollection]
    [java.util.concurrent Semaphore]
    [org.bson Document]
    [org.reactivestreams Subscriber]))

(defn get-collection []
  (-> (MongoClients/create)
      (.getDatabase "test")
      (.getCollection "test")))

(defn mk-doc [^String k v] (Document. k v))


(defn insert-one! [^MongoCollection coll key val callback]
  (-> (.insertOne coll (Document. key val))
      (.subscribe
        (reify Subscriber
          (onSubscribe [this sub] (.request sub 1))
          (onComplete [this] )
          (onNext [this data] (callback data))
          (onError [this t] (callback t))
          ))))

(defn insert-many! [^MongoCollection coll docs callback]
  (-> (.insertMany coll docs)
      (.subscribe
        (reify Subscriber
          (onSubscribe [this sub] (.request sub 1))
          (onComplete [this] )
          (onNext [this data] (callback data))
          (onError [this t] (callback t))
          ))))

(defn get-id [^MongoCollection coll id callback]
  (let [finished? (volatile! false)]
    (->
      (.find
        coll
        (com.mongodb.client.model.Filters/eq "_id" id))
      (.subscribe
        (reify Subscriber
          (onSubscribe [this sub] (.request sub 1))
          (onComplete [this]
            (when-not @finished?
              (callback ::no-result)))
          (onNext [this data]
            (vreset! finished? true)
            (callback data))
          (onError [this t] (callback t))
          )))))

(defn gen-uuid []
  (str (random-uuid)))

(defn insert-loop-batched! [batch-size]
  (let [coll (get-collection)
        sem (Semaphore. 10000)
        start-time (System/currentTimeMillis)
        target (long (/ 100000 batch-size))
        inserted (atom 0)]
    (loop [counter 0]
      (.acquire sem batch-size)
      (when (= 0 (mod counter target))
        (println "INSERTED" @inserted "in" (- (System/currentTimeMillis) start-time) "millis"))
      (insert-many!
        coll
        (repeatedly batch-size (fn [] (mk-doc "_id" (gen-uuid))))
        (fn [res]
          (.release sem batch-size)
          (swap! inserted + batch-size)
          (if (instance? Throwable res)
            (println "ERROR!" res))
          ))
       (recur (inc counter))
      )))

(defn insert-loop-batched-fixed! [num-pending batch-size start-id end-id]
  (let [coll (get-collection)
        sem (Semaphore. num-pending)
        target (long (/ 100000 batch-size))
        start-time (System/currentTimeMillis)
        inserted (atom 0)
        id-vol (volatile! start-id)]
    (loop [counter 0]
      (when (< @id-vol end-id)
        (.acquire sem batch-size)
        (when (= 0 (mod counter target))
          (println "INSERTED" @inserted "in" (- (System/currentTimeMillis) start-time) "millis"))
        (insert-many!
          coll
          (repeatedly batch-size (fn [] (mk-doc "_id" (str (vswap! id-vol + 2)))))
          (fn [res]
            (.release sem batch-size)
            (swap! inserted + batch-size)
            (if (instance? Throwable res)
              (println "ERROR!" res))
            ))
         (recur (inc counter))
        ))))

(defn mixed-read-write-load [num-pending start-id]
  (let [coll (get-collection)
        sem (Semaphore. num-pending)
        start-time (System/currentTimeMillis)
        id-vol (volatile! start-id)
        random (java.util.concurrent.ThreadLocalRandom/current)]
    (loop [counter 0]
      (.acquire sem 1)
      (when (= 0 (mod counter 100000))
        (println "FINISHED" counter "in" (- (System/currentTimeMillis) start-time) "millis"))
      (let [counter (atom 0)
            query-id (str (.nextLong random @id-vol))
            finished! (fn [res]
                        (when (instance? Throwable res)
                          (println "ERROR!" res))
                        (when (= 2 (swap! counter inc))
                          (.release sem 1)))
            insert-id (str (vswap! id-vol inc))]
        (insert-one!
          coll
          "_id"
          (str (vswap! id-vol inc))
          finished!)
        (get-id
          coll
          query-id
          finished!))
      (recur (inc counter)))
    ))
