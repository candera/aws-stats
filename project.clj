(defproject aws-stats "0.7.0"
  :description "Analyzes AWS S3 logfiles"
  :dependencies [[clj-aws-s3 "0.3.7"]
                 [com.datomic/datomic-free "0.9.4815"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/tools.namespace "0.2.4"]
                 [ring/ring-codec "1.0.0"]
                 [joda-time "2.6"]
                 [incanter "1.5.5"]]
  :main aws-stats.main
  :jvm-opts ["-Xmx4G"]
  :profiles
  {:dev
   {:init-ns user
    :source-paths ["dev"]}})
