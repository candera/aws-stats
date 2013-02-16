(ns aws-stats.main
  "Holds the external entry point of the program"
  (:require [aws-stats.analysis :as analysis]
            [aws-stats.database :as database]
            [aws-stats.ingest :as ingest]
            [clojure.pprint :refer [pprint]]
            [datomic.api :as d]))

(def usage
"Usage:

s32d ingest <s3-uri> <datomic-uri>
  Pulls log data from S3 at <s3-uri> into the database at
  <datomic-uri>

s32d report <report-name> <datomic-uri>

  Prints report of type <report-name> by querying database at
  <datomic-uri>. Currently only the \"download-equivalents\" report
  type is supported.
")

(defn invoke-report
  "Calls the appropriate analysis function with the current value of
  the database and prints the result."
  [report-name datomic-uri]
  (let [conn (d/connect datomic-uri)
        db   (d/db conn)
        report-sym (symbol report-name)
        analysis-ns (the-ns 'aws-stats.analysis)
        report-fn (ns-resolve analysis-ns report-sym)]
    (when-not report-fn
      (println "No such report" report-name)
      (println usage)
      2)
    (binding [*print-length* nil
              *print-level* nil]
      (pprint (report-fn db)))))

(defn -main [& args]
  (let [[command & args] args]
   (when-not command
     (println usage)
     (System/exit 1))
   (condp = (.toLowerCase command)
     "ingest"
     (let [[s3-uri datomic-uri] args
           peer (database/persistent-peer datomic-uri)]
       @(aws-stats.lifecycle/start peer)
       (let [conn (d/connect datomic-uri)]
        (ingest/ingest conn s3-uri)))

     "report"
     (let [[report-name datomic-uri] args]
       (invoke-report report-name datomic-uri))

     ;; Default case
     (do
       (println "Unrecognized command:" command)
       (println usage)
       (System/exit 1)))))
