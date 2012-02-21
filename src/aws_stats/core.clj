(ns aws-stats.core
  (:use [clojure.java.io :only (reader)]
        [clojure.pprint :only (pprint)]))

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


(def space " ")
(def delims [:owner nil space 
             :bucket nil space
             :time "[" "]"
             :remote-ip space space
             :requester nil space
             :request-id nil space
             :operation nil space
             :key nil space
             :request-uri "\"" "\""
             :status space space
             :error-code nil space
             :bytes-sent nil space
             :object-size nil space
             :total-time nil space
             :turnaround-time nil space
             :referrer "\"" "\""
             :user-agent "\"" "\""
             :version-id space nil])

(defn parse-line [line]
  (try
    (loop [m {}
          s line
          delims (partition 3 delims)]
     (if (empty? s)
       m
       (let [[k start-delim end-delim] (first delims)
             [match remainder] (between s start-delim end-delim)]
         (recur (assoc m k match)
                remainder
                (rest delims)))))
    (catch Throwable t
      {:unparseable t
       :content line})))



(defn is-file? [f]
  (.isFile f))

(defn has-extension? [f]
  (.contains (.getName f) "."))

(defn log-files [dir]
  (filter (complement has-extension?)
          (filter is-file? (seq (.listFiles (java.io.File. dir))))))

(defn lines [files]
  (apply concat (map (comp line-seq reader) files)))

(defn safe-decode [s]
  (try
    (Integer/decode s)
    (catch NumberFormatException _
      0)))


(defn -main [& args]
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


