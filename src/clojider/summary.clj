(ns clojider.summary)

;; To provide summary statistics, particularly for calculating the median value, we can't
;; return all request durations back as it's just too much data for something that should
;; be ready right after the test run. This is why we use a bucketing method for storing
;; the frequencies of each possible request duration. I.e. if there's five requests taking
;; 5ms, 3ms, 5ms, 6ms, 3ms we would only store a map of {5 2, 3 2, 6 1} which tells us that
;; there were 2 requests taking 5ms, 2 taking 3 ms and 1 taking 6 ms.
;;
;; In a simple example like this the method doesn't save space, but when there are millions
;; of requests, it starts to show as the possible request durations usally have a maximum
;; value and there are many "duplicate durations".
;;
;; For the purposes of this namespace:

;; requests has a shape of [{:name Int, :start Int, :end Int, :result Bool}]
;; summary has a shape of {[request name] {:ok-count Int, :ko-count Int, :durations Map}}
;; durations has a shape of {[millis] count}

(defn- request-count [durations]
  (reduce + (vals durations)))

(defn average [durations]
  ;; Calculates average duration of a bucketed duration map Millis -> Count
  ;; The total amount of millis is the sum of each Millis*Count
  ;; The total amount of requests is the sum of each Count
  (let [cnt (request-count durations)
        total-millis (reduce + (map #(* (first %) (second %)) durations))]
    (when (pos? cnt)
      (float (/ total-millis (request-count durations))))))

(defn- generate-value-index-ranges [durations]
  ;; For median calculations we need to know the duration of the Nth request. As we only
  ;; have frequencies of each duration, we can't just call (nth durations n) to get this.
  ;;
  ;; Instead, we map the durations as index ranges where they would be if we had a
  ;; sorted array of all durations. This is quite easy to do in a reduce loop:
  ;;
  ;; * start with start-idx = 0
  ;; * for each duration (sorted), return a range with duration, start-idx and
  ;;   end-idx = start-idx + frequency of the duration
  ;; * start again with start-idx = end-idx + 1 until we're done
  ;;
  ;; So for example, if we had durations {5 2, 3 2, 6 1} the ranges would be:
  ;;
  ;; [{:value 3 :start-idx 0 :end-idx 1}
  ;;  {:value 5 :start-idx 2 :end-idx 3}
  ;;  {:value 6 :start-idx 4 :end-idx 4}]
  ;;
  ;; Then, finding what value would be on the 3th index for example is quite
  ;; easy - just find the value of range where 3 >= start-idx && end-idx <= 3, in this
  ;; case being value 5
  (second
   (reduce
    (fn [[start-idx ranges] [millis cnt]]
      (let [end-idx (dec (+ start-idx cnt))]
        [(inc end-idx) (conj ranges {:value millis :start-idx start-idx :end-idx end-idx})]))
    [0 nil]
    (sort-by first durations))))

(defn- in-range? [{:keys [start-idx end-idx]} n]
  (<= start-idx n end-idx))

(defn- nth-value [ranges n]
  (->> ranges
       (filter #(in-range? % n))
       first
       :value))

(defn median [durations]
  (let [cnt (request-count durations)
        ranges (generate-value-index-ranges durations)]
    (cond
      ;; Can't find median if there are no values
      (= 0 cnt) nil

      ;; When there are even number of values, median is the
      ;; average of the two values closest to the halfway
      ;; in the sorted data set.
      (= 0 (mod cnt 2)) (let [half-idx (/ cnt 2)
                              sum (+ (nth-value ranges half-idx) (nth-value ranges (dec half-idx)))]
                          (/ sum 2))

      ;; Otherwise, median is the value closest to the center
      :else (let [half-idx (Math/floor (/ cnt 2))]
              (nth-value half-idx)))))

(defn kth-percentile [durations k]
  (cond
    (= k 100) (apply max (keys durations))
    (> k 100) (throw (IllegalArgumentException. "Can't calculate Kth percentile where K > 100%"))
    :else (let [cnt (request-count durations)
                ranges (generate-value-index-ranges durations)
                idx (* cnt (/ k 100))]
            ;; When the Kth percentile value index is a whole number i.e. idx % 1 == 0,
            ;; the Kth percentile is the average of the value at the index and the value
            ;; right next to it. Otherwise, we ceil up the number and that's the value
            ;; index.
            (if (= 0 (mod idx 1))
              (let [sum (+ (nth-value ranges (dec idx)) (nth-value ranges idx))]
                (/ sum 2))
              (nth-value ranges (Math/floor idx))))))
