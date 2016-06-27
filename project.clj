(defproject aws-stats "0.7.0"
  :description "Analyzes AWS S3 logfiles"
  :dependencies [[clj-aws-s3 "0.3.10"
                  :exclusions [joda-time]]
                 [com.datomic/datomic-free "0.9.5372"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.namespace "0.2.10"]
                 [ring/ring-codec "1.0.1"]
                 [joda-time "2.9.4"]
                 ;;[incanter "1.5.5"]
                 [incanter "1.9.1"]
                 ]
  :main aws-stats.main
  :jvm-opts ["-Xmx4G"]
  :profiles
  {:dev
   {:init-ns user
    :source-paths ["dev"]}})
