

(def stats (-> "datomic:free://localhost:4334/s3-stats" d/connect d/db downloads-by-day))

(def table (->> data
                (filter (fn [[object]] (.endsWith object ".mp3")))
                (reduce (fn [acc [object date downloads]]
                          (update-in acc [date object] #(+ downloads (or % 0))))
                        (sorted-map))))

(def data2
  (->> data
       (filter (fn [[object]] (.endsWith object ".mp3")))
       (map (fn [[object date downloads]]
              {:object object
               :date date
               :downloads downloads}))))

(require '[incanter.datasets])

(->> (incanter.datasets/get-dataset :co2)
     second)

(use '(incanter core stats charts datasets))

(with-data (get-dataset :co2)
  (view (bar-chart :Type :uptake
                   :title "CO2 Uptake"
                   :group-by :Treatment
                   :x-label "Grass Types" :y-label "Uptake"
                   :legend true)))


(def data (get-dataset :airline-passengers))
(view (bar-chart :year :passengers :group-by :month :legend true :data data))
(view (stacked-bar-chart :year :passengers :group-by :month :legend true :data data))

(let [chart (stacked-bar-chart :date :downloads
                               :group-by :object
                               ;;:legend true
                               :data ($order :date :asc
                                             (dataset [:object :date :downloads]
                                                      (->> stats
                                                           (filter (fn [[object date downloads]]
                                                                     (and (.endsWith object ".mp3")
                                                                          (< 50 downloads)
                                                                          (pos? (compare date "2014-01-01"))
                                                                          (pos? (compare "2014-12-01" date)))))
                                                           (map (fn [[object date downloads]]
                                                                  [object date (long downloads)]))))))]
  ;; Make the dates tiny on the X axis since they don't show up otherwise. Even better would be to make them go sideways.
  (-> chart
      .getCategoryPlot
      .getDomainAxis
      (.setTickLabelFont (java.awt.Font. "Arial" java.awt.Font/PLAIN 5)))
  (view chart))

(with-data  (get-dataset :airline-passengers)
  (view (bar-chart :month :passengers :group-by :year :legend true)))
