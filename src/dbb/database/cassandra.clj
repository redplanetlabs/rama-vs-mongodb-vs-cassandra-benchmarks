(ns dbb.database.cassandra
  (:import
    [com.datastax.oss.driver.api.core
      CqlSession
      CqlSessionBuilder]
    [com.datastax.oss.driver.api.core.config
      DefaultDriverOption
      DriverConfigLoader]
    [com.datastax.oss.driver.api.core.cql
      BatchStatement
      BatchStatementBuilder
      BatchType
      PreparedStatement
      Statement]
    [java.time Duration]
    [java.util.concurrent Semaphore]))

; CREATE KEYSPACE IF NOT EXISTS test
; WITH replication = {
;   'class': 'SimpleStrategy',
;   'replication_factor': 1
; };
;
; CREATE TABLE IF NOT EXISTS test.test (
;   pk text,
;   ck text,
;   value text,
;   PRIMARY KEY (pk, ck)
; );

(defn get-session []
  (let [cl (-> (DriverConfigLoader/programmaticBuilder)
               (.withDuration DefaultDriverOption/REQUEST_TIMEOUT (Duration/ofSeconds 30))
               .build)]
    (-> (CqlSessionBuilder.)
        (.withConfigLoader cl)
        .build)))

(defn prepare ^PreparedStatement [^CqlSession session ^String cql]
  (.prepare session cql))

(defn bind [^PreparedStatement ps & params]
  (.bind ps (into-array Object params)))

(defn execute! [^CqlSession session ^Statement s callback]
  (-> (.executeAsync session s)
      (.whenComplete
        (reify java.util.function.BiConsumer
          (accept [this t u]
            (callback (or t u))
            )))))

(defn prepare-insert ^PreparedStatement [^CqlSession session]
  (prepare session "INSERT INTO test.test (pk, ck, value) VALUES (?, ?, ?);"))

(defn prepare-query ^PreparedStatement [^CqlSession session]
  (prepare session "SELECT value FROM test.test WHERE pk = ? AND ck = ?;"))

(defn gen-uuid []
  (str (random-uuid)))

(defn insert-loop-batched! [num-pending batch-size]
  (let [session (get-session)
        insert (prepare-insert session)
        sem (Semaphore. num-pending)
        target (long (/ 100000 batch-size))
        start-time (System/currentTimeMillis)
        inserted (atom 0)]
    (loop [counter 0]
      (.acquire sem batch-size)
      (when (= 0 (mod counter target))
        (println "INSERTED" @inserted "in" (- (System/currentTimeMillis) start-time) "millis"))
      (let [^BatchStatementBuilder bsb
            (reduce
              (fn [^BatchStatementBuilder bsb i]
                (.addStatement bsb (bind insert (gen-uuid) (gen-uuid) (gen-uuid))))
              (BatchStatement/builder BatchType/LOGGED)
              (range 0 batch-size))
            ^BatchStatement bs (.build bsb)]
        (execute!
          session
          bs
          (fn [res]
            (.release sem batch-size)
            (swap! inserted + batch-size)
            (if (instance? Throwable res)
              (println "ERROR!" res)))))
       (recur (inc counter))
      )))

(defn insert-loop-batched-fixed! [num-pending batch-size start-id end-id]
  (let [session (get-session)
        insert (prepare-insert session)
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
        (let [^BatchStatementBuilder bsb
              (reduce
                (fn [^BatchStatementBuilder bsb i]
                  (let [id (str (vswap! id-vol + 2))]
                    (.addStatement bsb (bind insert id id id))))
                (BatchStatement/builder BatchType/LOGGED)
                (range 0 batch-size))
              ^BatchStatement bs (.build bsb)]
          (execute!
            session
            bs
            (fn [res]
              (.release sem batch-size)
              (swap! inserted + batch-size)
              (if (instance? Throwable res)
                (println "ERROR!" res)))))
         (recur (inc counter))
        ))))

(defn mixed-read-write-load [num-pending start-id]
  (let [session (get-session)
        insert (prepare-insert session)
        query (prepare-query session)
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
        (execute!
          session
          (bind insert insert-id insert-id insert-id)
          finished!)
        (execute!
          session
          (bind query query-id query-id)
          finished!))
      (recur (inc counter)))
    ))
