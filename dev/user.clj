(ns user
  "Namespace to support hacking at the REPL."
  (:require [aws.sdk.s3 :as s3]
            [aws-stats.database]
            [aws-stats.ingest :as ingest]
            [aws-stats.lifecycle]
            [clojure.repl :refer [doc]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.repl :refer [refresh]]
            [datomic.api :as d :refer [q]])
  (:import [aws_stats.lifecycle Lifecycle]))

;; Do not create directly; use dev-system function
(defrecord DevSystem [peer]
  aws-stats.lifecycle/Lifecycle
  (start [_]
    (future (log/info :STARTING "dev-system")
            @(aws-stats.lifecycle/start peer)))
  (stop [_]
    (future (log/info :STOPPING "dev-system")
            @(aws-stats.lifecycle/stop peer))))

(defn dev-system
  "Returns a complete system in :dev mode for development at the REPL."
  []
  (let [peer (aws-stats.database/temp-peer)]
    (map->DevSystem {:peer peer})))

;;; Development system lifecycle

(def system
  "Global singleton reference to the entire system, for use only when
  developing at the REPL."
  nil)

(defn init
  "Initializes the development system."
  []
  (alter-var-root #'system (constantly (dev-system))))

(defn go
  "Launches the development system. Ensure it is initialized first."
  []
  (when-not system (init))
  @(aws-stats.lifecycle/start system)
  (set! *print-length* 20)
  :started)

(defn stop
  "Shuts down the development system and destroy all its state."
  []
  (when system
    @(aws-stats.lifecycle/stop system)
    (alter-var-root #'system (constantly nil)))
  :stopped)

(defn reset
  "Stops the currently-running system, reload any code that has changed,
  and restart the system."
  []
  (stop)
  (refresh :after 'user/go))

;;; Development-time convenience functions

(defn conn
  "Returns the current database connection for the development system."
  []
  (d/connect (:uri (:peer system))))

(defn db
  "Returns the most-recent database value for the development system."
  []
  (d/db (conn)))

;; Grab my AWS keys, but not if the file isn't present - we .gitignore
;; it soas not to check it in.

(try (require '[private :refer [access-key secret-key test-bucket]])
     (catch Throwable _
       (def access-key nil)
       (def secret-key nil)))

;;; aws-stats stuff

(defn doit
  []
  (let [creds (ingest/creds access-key secret-key)
        object-keys (ingest/object-keys creds "www.wangdera.com-logs" nil)
        key (first object-keys)
        r (ingest/reader-for-object creds "www.wangdera.com-logs" key)
        lines (line-seq r)
        entries (map ingest/parse-line lines)
        ;;entry-txdata (map #(ingest/logentry-entity-data 1234 %) entries)
        ]
    entries))

(def test-bucket)

(defn ingest []
  (let [s3-uri (str test-bucket
                    "?aws_access_key=" access-key
                    "&aws_secret_key=" secret-key)]
    (println "ingesting" s3-uri)
    (ingest/ingest (conn) s3-uri)))

(comment

(deref (d/transact (conn) [{:db/id (d/tempid :aws-stats.part/core)
                      :aws-stats/bucket-name "test"}]))

(q '[:find ?e
     :where
     [?e :aws-stats/bucket-name "test"]]
   (db))

)

(defn day
  "Returns the day of an inst"
  [inst]
  (-> inst org.joda.time.DateTime. (.toString "YYYY-MM-dd")))


(defn downloads-by-day
  "Returns a map of S3 URIs to the equivalent number of times it has
  been downloaded by day. A download equivalent is the number of bytes
  transferred divided by the object size."
  [db]
  (->> (d/q '[:find ?uri ?day (sum ?download)
              ;; We need a :with because we don't want to sum
              ;; *unique* downloads, but *all* downloads
              :with ?request-id
              :where
              [?entry :s3.stat/bytes-sent ?bytes-sent]
              [?entry :s3.stat/object-size ?object-size]
              [?entry :s3.stat/key ?key]
              [?entry :s3.stat/bucket ?bucket]
              [?entry :s3.stat/request-id ?request-id]
              [?entry :s3.stat/time ?time]
              [(str "s3://" ?bucket "/" ?key) ?uri]
              [(user/day ?time) ?day]
              [(double ?bytes-sent) ?bytes-double]
              [(/ ?bytes-double ?object-size) ?download]]
            db)
       (into [])))
