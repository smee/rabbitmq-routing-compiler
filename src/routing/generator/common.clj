(ns routing.generator.common)

(defn as-flat-set 
  "Find all maps in an arbitrarly nested data structure (sets, vectors, lists, sequences)
and converts it to a set of maps."
  [& stuff]
  (->> stuff
;    (apply concat)
    (clojure.walk/postwalk #(if (set? %) (vec %) %))
    flatten
    (remove nil?)
    set))