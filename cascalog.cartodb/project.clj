(defproject cascalog-more-taps/cascalog.cartodb "0.0.1" 
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.9.0"]
                 [cascading/cascading-hadoop "2.0.5-wip-344"]
                 [org.pingles/cascading.protobuf "0.0.1"]
                 [cartodb-java-client "1.0.1"]
                 [com.google.guava/guava "12.0"]]
  :source-paths ["src/clj"]
  :profiles {:dev
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
               [lein-javac "1.2.1-SNAPSHOT"]]}}
  :repositories {"conjars" "http://conjars.org/repo/"}
  :java-source-paths ["src/jvm"]
  :jvm-opts ["-XX:MaxPermSize=128M"
             "-XX:+UseConcMarkSweepGC"
             "-Xms1024M"
             "-Xmx1048M"
             "-server"]
  :min-lein-version "2.0.0"
  :plugins [[lein-swank "1.4.4"]
            [lein-clojars "0.9.0"]]
  :description "CartoDB Cascalog Tap.")
