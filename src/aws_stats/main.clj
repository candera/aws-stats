(ns aws-stats.main
  "Holds the external entry point of the program")

(defn -main [& args]
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
       (println (apply format fmt result))))))


