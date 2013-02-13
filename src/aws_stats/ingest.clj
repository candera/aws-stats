(ns aws-stats.ingest
  (:require [aws.sdk.s3 :as s3]
            [aws-stats.database :as database]
            [aws-stats.util :as util]
            [clojure.java.io :refer [reader file]]
            [clojure.string :as str]
            [datomic.api :as d]))

(defn between
  ([s delim] (between delim delim s))
  ([s start-delim end-delim]
     (let [n-start (if start-delim (inc (.indexOf s start-delim)) 0)
           n-end (when end-delim (.indexOf s end-delim (inc n-start)))]
       (if n-end
         [(.substring s n-start n-end)
          (.substring s (inc n-end))]
         [(.substring s n-start)
          nil]))))


(defn to-long
  "Parses a string into an integer, or into nil if it is empty or a dash."
  [s]
  (cond
   (str/blank? s) nil
   (= s "-") nil
   :else (Long/parseLong s)))

(defn to-inst
  "Parses a string into a java.util.Date."
  [s]
  (.parse (java.text.SimpleDateFormat. "dd/MMM/yyyy:HH:mm:ss zzzz")
          s
          (java.text.ParsePosition. 0)))

(def space " ")
(def dq "\"")
(def delims [:owner nil space identity
             :bucket nil space identity
             :time "[" "]" to-inst
             :remote-ip space space identity
             :requester nil space identity
             :request-id nil space identity
             :operation nil space identity
             :key nil space identity
             :request-uri dq dq identity
             :status space space to-long
             :error-code nil space identity
             :bytes-sent nil space to-long
             :object-size nil space to-long
             :total-time nil space to-long
             :turnaround-time nil space to-long
             :referrer dq dq identity
             :user-agent dq dq identity
             :version-id space nil identity
])

(defn parse-line [line]
  (loop [m      {}
         s      line
         delims (partition 4 delims)]
    (if (empty? s)
      m
      (let [[k start-delim end-delim f] (first delims)
            [match remainder] (between s start-delim end-delim)]
        (recur (assoc m k (f match))
               remainder
               (rest delims))))))

(defn safe-decode [s]
  (try
    (Integer/decode s)
    (catch NumberFormatException _
      0)))

(defn creds
  "Returns a credentials map suitable for use with the S3 library."
  [access-key secret-key]
  {:access-key access-key
   :secret-key secret-key})

(defn reader-for-object
  "Returns a reader over the content of object named by `key` in `bucket`."
  [creds bucket key]
  (-> (s3/get-object creds bucket key)
      :content
      reader))

(defn object-keys
  "Returns all the keys in `bucket`, limiting them to those that start
  with `prefix`, if it is non-nil."
  [creds bucket prefix]
  (let [opts (if prefix {:prefix prefix} {})]
    (map :key (:objects (s3/list-objects creds bucket opts)))))

(defn without-nil-values
  "Returns a map without entries where the value is nil"
  [m]
  (reduce-kv (fn [a k v] (if (nil? v) a (assoc a k v)))
             {}
             m))

(defn logentry-entity-data
  "Converts a log entry into a map that can be transacted"
  [logfile-eid
   {:keys [owner
           bucket
           time
           remote-ip
           requester
           request-id
           operation
           key
           request-uri
           status
           error-code
           bytes-sent
           object-size
           total-time
           turnaround-time
           referrer
           user-agent
           version-id]}]
  (without-nil-values
   {:db/id (d/tempid :aws-stats.part/core)
    :aws-stats/logfile logfile-eid
    :aws-stats/owner owner
    :aws-stats/entry-bucket bucket
    :aws-stats/time time
    :aws-stats/remote-ip remote-ip
    :aws-stats/requester requester
    :aws-stats/request-id request-id
    :aws-stats/operation operation
    :aws-stats/key key
    :aws-stats/request-uri request-uri
    :aws-stats/status status
    :aws-stats/error-code error-code
    :aws-stats/bytes-sent bytes-sent
    :aws-stats/object-size object-size
    :aws-stats/total-time total-time
    :aws-stats/turnaround-time turnaround-time
    :aws-stats/referrer referrer
    :aws-stats/user-agent user-agent
    :aws-stats/version-id version-id}))

(defn split-last
  "Splits string `s` at the last occurrence of `sep`, returning a
  vector containing the two parts."
  [sep s]
  (let [i (inc (.lastIndexOf s sep))]
    (if (pos? i)
      [(subs s 0 i) (subs s i)]
      [s])))

(defn nil-if
  "Returns nil if s1 is equal to s2. Returns s1 otherwise."
  [s1 s2]
  (when-not (= s1 s2) s1))

(defn parse-s3-uri
  "Given an s3 URI like s3://bucket/prefix/key, return the URI as a
  map with keys :protocol, :bucket, :prefix, and :key. `options` is a
  map which may contain `:no-key`, in which case the trailing segment
  should be treated as the entirety of the prefix. So in the above
  example, `prefix/key` would be returned as the :prefix, and no :key
  would be returned."
  [uri & {:keys [no-key]}]
  (let [u (java.net.URI. uri)]
    (when (not= "s3" (.getScheme u))
      (throw (ex-info (str "URI is not an S3 URI:" uri)
                      {:reason :not-s3-uri
                       :uri uri})))
    (merge {:bucket (.getHost u)}
           (if no-key
             {:prefix (nil-if "/" (.getPath u))}
             (let [[prefix key] (split-last "/" (.getPath u))]
               {:prefix (nil-if "/" prefix)
                :key key})))))

(defn logsource-eid
  "Find or create the logsource entity for `uri`. Returns the entity
  ID."
  [conn uri]
  (let [tempid (d/tempid :aws-stats.part/core)
        tx-result @(d/transact conn [{:db/id tempid
                                      :aws-stats/logsource-uri uri}])]
    (:db/id (database/tx-ent tx-result tempid))))

(defn ingested-logfiles
  "Returns a set of identifiers for logfiles that have been ingested
  from the bucket identified by log-source-eid."
  [conn logsource-eid]
  (->> (d/q '[:find ?logfile-identifier
              :in $ ?logsource
              :where
              [?logfile :aws-stats/logsource ?logsource]
              [?logfile :aws-stats/logfile-identifier ?logfile-identifier]]
            (d/db conn)
            logsource-eid)
       (map first)
       set))

(defn logfile-identifier
  "Create a logfile identifier given a base S3 URI, and key."
  [s3-uri key]
  (if (.endsWith s3-uri "/")
    (str s3-uri key)
    (str s3-uri "/" key)))

(defn ingest
  "Imports the S3 log files living at `s3-uri` into the database using
  connection `conn`. Does not ingest a file if it has already been
  ingested."
  [conn s3-uri access-key secret-key]
  (let [creds                   (creds access-key secret-key)
        {:keys [bucket prefix]} (parse-s3-uri s3-uri :no-key true)
        logsource-eid          (logsource-eid conn s3-uri)
        object-keys             (object-keys creds bucket prefix)
        ingested-logfiles       (ingested-logfiles conn logsource-eid)]
    (println "Found" (count object-keys) "in" bucket prefix)
    (doseq [key object-keys]
      (if (ingested-logfiles (logfile-identifier s3-uri key))
        (println "Skipping" key "because it has already been ingested")
        (let [r (reader-for-object creds bucket key)
              logfile-eid (d/tempid :aws-stats.part/core)]
          (println "Ingesting" key)
          (try
            (let [logfile-identifier (logfile-identifier s3-uri key)
                  logfile-txdata [{:db/id logfile-eid
                                   :aws-stats/logsource logsource-eid
                                   :aws-stats/logfile-identifier logfile-identifier}]
                  logentry-txdata (->> (line-seq (reader-for-object creds bucket key))
                                       (map parse-line)
                                       (map #(logentry-entity-data logfile-eid %)))]
              @(d/transact conn (concat logfile-txdata
                                        logentry-txdata)))
            (catch Throwable t
              (println "Unable to ingest" key "because of" t))
            (finally
              (.close r))))))))


;; Here's what used to be our main function
(comment

  (let [stats (map parse-line (lines (log-files (first args))))
        successes (filter #(= "200" (:status %)) stats)
        by-key (group-by :key successes)
        ;; TODO: make this readable by humans (including me)
        results (reverse (sort-by second (map (fn [[k v]] (let [size (safe-decode (:object-size (first v)))
                                                                total-bytes (reduce + (map #(safe-decode (:bytes-sent %)) v))]
                                                            [k (count v) (when (not= 0 size) (float (/ total-bytes size)))]))
                                              by-key)))
        max-key-length (reduce max (map count (keys by-key)))
        fmt (str "%" max-key-length "s %10s %10s")]
    (println (format fmt "object" "requests" "equivalent downloads"))
    (doseq [result results]
      (println (apply format fmt result)))))
