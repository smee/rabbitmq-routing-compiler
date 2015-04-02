(ns routing.tests.logging-client
  (:require [common.rabbit :refer [with-connection]]
            [clojure.walk]
            [langohr 
             [core      :as rmq]
             [channel   :as lch]
             [queue     :as lq]
             [consumers :as lc]
             [basic     :as lb]]
            [clojure.tools.logging :as log :refer [info warn debug trace infof error]]))

(defn logging-consumer [name ch headers ^bytes payload]
  (infof "[%s] headers='%s', payload='%s'" name (pr-str headers) (pr-str (String. payload "utf8"))))

(defn- remove-nils [m]
  (into {} (keep (fn [[k v]] (when v [k v])) m)))

(defonce tracing-msgs (atom []))

(defn- remove-rmq-internal-classes 
  "There is a java map instance in there with instances of internal private classes etc.
	We need to traverse the data structure and convert everything to something more printer friendly.
	Look at you, com.rabbitmq.client.impl.LongStringHelper$ByteArrayLongString ...
	But since clojure.walk doesn't walk java's data structures, we need to convert them first."
  [m]
  (clojure.walk/prewalk #(cond 
                           (instance? java.util.HashMap %) (into {} %)
                           (instance? java.util.ArrayList %) (into [] %)
                           (.contains (str (class %)) "ByteArray") (str %)
                           :else %) m))

(defn logging-consumer-wo-payload [name ch headers ^bytes payload]
  (let [len (alength payload)
        hdrs (remove-rmq-internal-classes (remove-nils (:headers headers)))
        ^String rk (:routing-key headers)
        ] 
    #_(swap! tracing-msgs conj {:name name
                                :vhost vhost
                                :payload-length len 
                                :headers hdrs}) 
    (infof "[%s] RK=%s, Payload size=%d, Headers=%s" name rk len (pr-str hdrs))))


