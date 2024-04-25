(defproject com.rpl/database-benchmarks "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.mongodb/mongodb-driver-reactivestreams "5.0.1"]
                 [com.datastax.oss/java-driver-core "4.17.0"]]
  :aliases
  {"rebl"
   ["with-profile" "+rebl" "trampoline" "run" "-m" "rebel-readline.main"]}
  :profiles {:dev {:resource-paths ["test/resources/"]}
             :provided {:dependencies [[com.rpl/rama "0.12.1"]]}
             :rebl      {:dependencies [[com.bhauman/rebel-readline "0.1.4"]]}})
