(ns routing.contracts
  (:require [org.clojars.smee
             [map :refer [map-values]]
             [seq :refer [find-where]]]
            [clojure.set :refer [difference]]
            [schema.core :as s]
            [routing.schemas :refer :all]
            [routing.generator.rabbit-password :refer [rabbit-password-hash]]))



(defn pw [s]
  (rabbit-password-hash s (byte-array (map byte [1 2 3 4]))))


(def merge-contracts (partial org.clojars.smee.map/deep-merge-with into))

(defn generate-routing-backend-partial-contract ;;FIXME
  "Create contract data structures for a backend service. The result can be merged into the
`+Contracts+` data structure. Since the result does not contain user specifications nor exchanges, 
it can only be validated against
`(dissoc +Contracts+ :users :exchanges)`"
  [users {:keys [backend-user queue-name tag]}]
  (->> users
    (map-indexed (fn [idx user]
                   (let [uuid (str (java.util.UUID/randomUUID))
                         uuid-answer (str (java.util.UUID/randomUUID))] 
                     {:covenants {uuid {:from user
                                        :to backend-user
                                        :tag tag}
                                  uuid-answer {:from backend-user
                                               :to user
                                               :tag tag}}
                      :collections {user #{uuid-answer}
                                    "ALL" #{uuid-answer uuid}}
                      :users {user {:queues #{queue-name}
                                    :allocations {uuid-answer #{queue-name}}}
                              backend-user {:queues #{queue-name}
                                            :allocations {uuid #{queue-name}}}}})))
    (reduce merge-contracts)))

(comment 
  (clojure.pprint/pprint
    ;; merge existing contracts, new platform user, and access to routing backend for user 'analysis'
    (merge-contracts @contracts 
                     {:users {"platform" {:name "platform" 
                                          :password (pw "platform")
                                          :exchange "platform-ex-write" 
                                          :queues #{"control"}}}}
                     (generate-routing-backend-partial-contract 
                       ["analysis"] 
                       {:backend-user "platform"
                        :queue-name "control"
                        :tag "control.routing"})))) 

(defn enumerate-routing-keys 
  "Generate all valid routing keys from contracts data."
  ([contracts] (enumerate-routing-keys contracts nil))
  ([{:keys [covenants collections]} user]
  (set 
    (for [[cc-id ids] collections, c-id ids,
          :let [{from :from tag :tag} (get covenants c-id)]
          :when (or (nil? user) (= from user))]
      (format "%s.%s.%s" from tag cc-id)))))

;;;;;;;;;;;;;;;; Exemplary contracts demonstrating different features ;;;;;;;;;;;;;;;;;;;;;;

(def empty-contracts "no contracts at all" 
  {:users {}
   :covenants {}
   :collections {}})

(def demo-contracts "some demo contracts in the application domain of power plant maintenance" 
  {:users {"archiver" {:name "archiver"
                       :password (pw "")
                       :queues #{"archiver-q-0"}
                       :exchange "archiver-ex-write"
                       :allocations {"1" #{"archiver-q-0"}
                                     "2" #{"archiver-q-0"}}} 
           "planning" {:name "planning" 
                       :password (pw "planning")
                       :queues #{"planning-q-0"}
                       :exchange "planning-ex-write"
                       :allocations {"3" #{"planning-q-0"}
                                     "7" #{"planning-q-0"}}} 
           "scada" {:name "scada" 
                    :password (pw "scada")
                    :queues #{"scada-q-0"}
                    :exchange "scada-ex-write"
                    :allocations {"10" #{"scada-q-0"}}}
           "analysis" {:name "analysis" 
                       :password (pw "analysis")
                       :queues #{"analysis-q-0"}
                       :exchange "analysis-ex-write"
                       :allocations {"5" #{"analysis-q-0"}}}
           "billing" {:name "billing" 
                      :password (pw "billing")
                      :queues #{"billing-q-0"}
                      :exchange "billing-ex-write"
                      :allocations {"6" #{"billing-q-0"}}}}
   :covenants {"1" {:from "scada" 
                    :to "archiver" 
                    :tag "data"}
               "2" {:from "analysis" 
                    :to "archiver" 
                    :tag "storedata"}
               "3" {:from "scada" 
                    :to "planning" 
                    :tag "alarm"}
               "5" {:from "scada" 
                    :to "analysis" 
                    :tag "data"}
               "6" {:from "planning" 
                    :to "billing" 
                    :tag "angebot"}
               "7" {:from "billing" 
                    :to "planning" 
                    :tag "angebot"}
               "10" {:from "analysis" 
                     :to "scada" 
                     :tag "storedata"}} 
   :collections {"data-to-store" #{"1"}
                 "data-to-analysis" #{"5"}
                 "ALL" #{"1" "2" "3" "5" "6" "7" "10"}
                 "just-data" #{"1" "2" "3" "5" "6" "7" "10"}}})

(def demo-local-users "delegate covenants to a local user (subordinate of a platform user)"
  (merge-contracts demo-contracts 
                   {:users {"scada" {:localusers {"scada-sub1" {:name "scada-sub1"
                                                               :password (pw "scada-sub1")
                                                               :exchange "scada-sub1-ex-write"
                                                               :queues #{"scada-q-0"}
                                                               :delegation #{"1" "3"}}}}}})) 

(def demo-remote-contracts "proxy to remote rabbitmq instance using a similar routing-compiler" 
  (merge-contracts demo-contracts
                   {:users {"proxyuser" {:name "proxyuser" 
                                         :password (pw "proxyuser")
                                         :queues #{"proxyuser-q-0"}
                                         :exchange "proxyuser-ex-write" 
                                         :allocations {"remote-2" #{"proxyuser-q-0"}}
                                         :remote {:aliases #{"foo" "bar"} ; platform users of remote RabbitMQ instance
                                                  :remote-uri "amqp://USERNAME:PASSWORD@REMOTE-HOST:PORT/VHOST"
                                                  :exchange "USERNAME-ex-write" 
                                                  :queue "USERNAME-q-0"}}
                            "scada" {:allocations {"remote-1" #{"scada-q-0"}}}}
                    :covenants {"remote-1" {:from "foo" 
                                            :to "scada" 
                                            :tag "diagnosis"}
                                "remote-2" {:from "scada" 
                                            :to "bar" 
                                            :tag "storedata"}}
                    :collections {"ALL" #{"remote-1" "remote-2"}}}))

(def demo-delegation "delegation of covenants between platform users"
  (merge-contracts demo-contracts
                   {:users {"analysis" {:delegation {"archiver" #{"quali-1" "quali-2"}}
                                        :allocations {"quali-1" #{"analysis-q-for-archiver"}}
                                        :queues #{"analysis-q-for-archiver"}}}
                    :covenants {"quali-1" {:from "scada" :to "archiver" :tag "testQuality"}
                                "quali-2" {:from "archiver" :to "scada" :tag "qualityResults"}}
                    :collections {"ALL" #{"quali-1" "quali-2"}}}))

;;;;;;;;;;;;;;;; contract related public API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defonce contracts (atom demo-contracts))

(s/defn replace-contracts!
  [data :- +Contracts+]
  (reset! contracts data))

(s/defn add-user! 
  [{n :name :as user} :- +PlatformUser+]
  (swap! contracts assoc n user))


(defn covenant-ids-where-sender 
  [contracts user]
  (set (keep (fn [[id {f :from}]] (when (= user f) id)) (:covenants contracts))))

(defn covenant-ids-of 
  [contracts user] 
  (set (keep (fn [[id {f :from t :to}]] (when (or (= user f) (= user t)) id)) (:covenants contracts))))

(defn- delete-user-internal 
  [contract user]
  (let [cov-ids (covenant-ids-of contract user)]
    (-> contract
      (update-in [:users] dissoc user)
      (update-in [:collections] (partial clojure.walk/postwalk #(if (set? %) (difference % cov-ids) %)))
      (update-in [:covenants] (partial reduce-kv (fn [m k v] (if (cov-ids k) m (assoc m k v))) {})))))


(defn delete-user! 
  [user]
  (swap! contracts delete-user-internal user))


(s/defn add-covenant-collection! 
  [id :- +CovenantCollectionId+ 
   coll :- #{+CovenantId+}]
  (swap! contracts assoc-in [:collections id] coll))


(defn delete-covenant-collection! 
  [id]
  (swap! contracts update-in [:collections] dissoc id))


(s/defn add-covenant! :- +Contracts+ 
  [cov :- +Covenant+]
  (swap! contracts assoc-in [:covenants (str (java.util.UUID/randomUUID))] cov)) 

(defn- delete-covenant-internal [contracts cov-id]
  (-> contracts
    (update-in [:collections] (partial map-values #(disj % cov-id)))
    (update-in [:covenants] dissoc cov-id)
    (update-in [:users] (partial map-values #(update-in % [:allocations] dissoc cov-id)))))

(defn delete-covenant! [id]
  (swap! contracts delete-covenant-internal id))


(s/defn reset-allocations! :- +Contracts+ 
  [user :- s/Str
   allocations :- +Allocations+]
  (swap! contracts assoc-in [:allocations user] allocations))


(defn user-view 
  "Select only the information in `contracts` that relates to the platform user `pf-user`."
  [contracts pf-user]
  (let [cov-ids (set (covenant-ids-of contracts pf-user))] 
    {:users {pf-user (-> contracts :users (get pf-user))}
     :covenants (select-keys (:covenants contracts ) cov-ids)
     :collections (map-values (partial clojure.set/intersection cov-ids) (:collections contracts))}))

