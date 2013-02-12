(ns aws-stats.database
  "Database operations."
  (:refer-clojure :exclude [ensure partition])
  (:require [aws-stats.lifecycle]
            [aws-stats.util :as util]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [datomic.api :as d])
  (:import [aws_stats.lifecycle Lifecycle]))

(defn- prop
  "Return a key-value pair appropriate to the value passed in. For
  instance, for :db.cardinality/one, return
  [:db/cardinality :db.cardinality/one]"
  [v]
  (case (-> v namespace keyword)
    :db.cardinality [:db/cardinality v]
    :db.unique      [:db/unique v]
    ))

(defn- entity
  "Returns transaction data for an entity in the db partition."
  [ident doc]
  {:db/id    (d/tempid :db.part/db)
   :db/ident ident 
   :db/doc   doc})

(defn- attribute 
  "Returns transaction data for a Datomic attribute definition with the specified
  properties."
  [name doc type & props]
  (into {:db/id                 (d/tempid :db.part/db)
         :db/ident              name
         :db/valueType          type
         :db/doc                doc
         :db/cardinality        :db.cardinality/one ; Default
         :db.install/_attribute :db.part/db}
        (map prop props)))

(defn- partition
  "Returns transaction data for creating a Datomic partition."
  [ident docstring]
  {:db/ident              ident
   :db/doc                docstring
   :db/id                 (d/tempid :db.part/db)
   :db.install/_partition :db.part/db})

;; Our schema consists of an entity for the log source, which has a
;; URI and a relationship with zero or more logfile entities. A
;; logfile entity has a key and a relationship with zero or more
;; stats. A stat has a whole bunch of attributes like time, request
;; uri, and so forth.

(def schemas
  {:core
   [[(partition :aws-stats.part/core "The main partition where we'll put our stuff.")]
    [(attribute :aws-stats/logsource-uri
                "The URI an s3 location containing log files. Looks like s3://bucket-name/prefix"
                :db.type/string
                :db.unique/identity)
     (attribute :aws-stats/logsource
                "The S3 location logfiles were pulled from."
                :db.type/string)
     (attribute :aws-stats/logfile
                "A reference to an S3 logfile"
                :db.type/ref)
     (attribute :aws-stats/logfile-identifier
                "An identifier for a logfile. Looks like s3://bucket-name/prefix/key"
                :db.type/string
                :db.unique/identity)
     (attribute :aws-stats/logfile-key
                "The S3 object key identifying a logfile. Looks like 2013-02-03-14-19-52-0FC24E4384112C87"
                :db.type/string)
     (attribute :aws-stats/entry-bucket
                "The bucket of the object this log entry is talking about. Not to be
  confused with the bucket where we got the logfile."
                :db.type/string)
     (attribute :aws-stats/owner
                "The owner of the object"
                :db.type/string)
     (attribute :aws-stats/time
                "The time of the request"
                :db.type/instant)
     (attribute :aws-stats/remote-ip
                "The IP address the request came from"
                :db.type/string)
     (attribute :aws-stats/requester
                "The identity of whomever made the request"
                :db.type/string)
     (attribute :aws-stats/request-id
                "The unique ID of the request"
                :db.type/string)
     (attribute :aws-stats/operation
                "The operation that was performed"
                :db.type/string)
     (attribute :aws-stats/key
                "The key of the object."
                :db.type/string)
     (attribute :aws-stats/request-uri
                "The full request URI, including verb and HTTP version."
                :db.type/string)
     (attribute :aws-stats/status
                "The HTTP status code returned for this request."
                :db.type/long)
     (attribute :aws-stats/error-code
                "The error code returned for this request."
                :db.type/string)
     (attribute :aws-stats/bytes-sent
                "The number of bytes returned with this request."
                :db.type/long)
     (attribute :aws-stats/object-size
                "The size of the object requested."
                :db.type/long)
     (attribute :aws-stats/total-time
                "The time spent servicing this request, in milliseconds"
                :db.type/long)
     (attribute :aws-stats/turnaround-time
                "No idea what this one is"
                :db.type/long)
     (attribute :aws-stats/referrer
                "The referrer"
                :db.type/string)
     (attribute :aws-stats/user-agent
                "The user agent"
                :db.type/string)
     (attribute :aws-stats/version-id
                "The version ID"
                :db.type/string)]]
   
   ;; We could have other schemas in here as well
   ;; :other-schema
   ;; [[(attribute :aws-stats/something-else
   ;;              "Description"
   ;;              :db.type/string
   ;;              :db.cardinality/one
   ;;              :db.unique/identity)]]
   })

(defn ensure
  "Asserts additional schema stored at key."
  [conn key]
  (doseq [tx (util/getx schemas key)]
    @(d/transact conn tx)))

(defn init-db
  [conn]
  (ensure conn :core))

;; Do not create directly; use temp-peer function
(defrecord TemporaryPeer [uri]
  Lifecycle
  (start [_]
    (future
      (log/info :STARTING "temporary-peer" :uri uri)
      (d/create-database uri)
      (let [conn (d/connect uri)]
        (init-db conn))))
  (stop [_]
    (future
      (log/info :STOPPING "temporary-peer" :uri uri)
      (d/delete-database uri))))

(defn temp-peer
  "Returns an object implementing the Lifecycle protocol for a
  temporary, in-memory Datomic database. Suitable for development and
  testing. Creates a uniquely-named database on startup, asserts the
  schema and sample data. Deletes the database on shutdown. The
  Datomic database URI is available as the key :uri."
  []
  (let [name (d/squuid)
        uri (str "datomic:mem:" name)]
    (->TemporaryPeer uri)))

;; Do not create directly; use persistent-peer function
(defrecord PersistentPeer [uri memcached-nodes]
  Lifecycle
  (start [_]
    (future
      (log/info :STARTING "persistent-peer" :uri uri :memcached-nodes memcached-nodes)
      (when-not (str/blank? memcached-nodes)
        (System/setProperty "datomic.memcacheServers" memcached-nodes))
      (d/create-database uri)
      (let [conn (d/connect uri)]
        (init-db conn))))
  (stop [_]
    (log/info :STOPPING "persistent-peer" :uri uri)
    (future nil)))

(defn persistent-peer
  "Returns an object implementing the Lifecycle protocol for a
  persistent Datomic database using the given URI and memcached nodes
  (which may be blank). Ensures on startup that the database has been
  created and the schema has been asserted."
  ([uri]
     (persistent-peer uri ""))
  ([uri memcached-nodes]
     (->PersistentPeer uri memcached-nodes)))

(defn tx-ent
  "Resolve entity id to entity as of the :db-after value of a tx result"
  [txresult eid]
  (let [{:keys [db-after tempids]} txresult]
    (d/entity db-after (d/resolve-tempid db-after tempids eid))))
