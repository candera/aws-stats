(defproject aws-stats "0.7.0"
  :description "Analyzes AWS S3 logfiles"
  :dependencies [[clj-aws-s3 "0.3.3"]
                 [com.datomic/datomic-free "0.8.3789"]
                 [org.clojure/clojure "1.5.0-beta12"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/tools.namespace "0.2.2"]
                 [ring/ring-codec "1.0.0"]]
  :main aws-stats.main
  :profiles
  {:dev
   {:source-paths ["dev"]}})
