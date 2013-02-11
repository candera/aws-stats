(ns aws-stats.util
  "The inevitable catch-all namespace.")

(defn getx 
  "Just like clojure.core/get, but it throws an exception if the entry
  is not found."
  [m k]
  (let [v (get m k ::not-found)]
    (if (= v ::not-found)
      (throw (ex-info "Couldn't find key"
                      {:reason :could-not-find-key
                       :map m
                       :key k}))
      v)))

(defn not-implemented
  "Returns an exception that indicates the method is not implemented"
  []
  (ex-info "Not yet implemented"
           {:reason :not-yet-implemented}))

