(defproject aws-stats "1.0.0-SNAPSHOT"
  :description "Analyzes AWS S3 logfiles"
  :dependencies [[org.clojure/clojure "1.4.0"]]
  :main aws-stats.core
  :aot [aws-stats.core])