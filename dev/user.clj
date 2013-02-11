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

(try (require '[creds :refer [access-key secret-key]]))

;;; aws-stats stuff

(comment

(deref (d/transact (conn) [{:db/id (d/tempid :aws-stats.part/core)
                      :aws-stats/bucket-name "test"}]))

(q '[:find ?e 
     :where 
     [?e :aws-stats/bucket-name "test"]]
   (db))


)
