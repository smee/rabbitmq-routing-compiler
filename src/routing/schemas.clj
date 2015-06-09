(ns ^{:doc "Type annotations for core data structures. Uses Prismatic's schema library."}
routing.schemas
  (:require [schema.core :as s :refer [defschema]]))

;;;;;;;;;;;;;; Schema Definitions for Contracts ;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def +UserName+ s/Str)
(def +CovenantId+ s/Str)
(def +CovenantCollectionId+ s/Str)

(defschema +Queues+ #{s/Str})

(defschema +Allocations+ {s/Str #{s/Str}})

(defschema +LocalUser+
  {:name +UserName+
   :password s/Str
   :queues +Queues+
   :exchange s/Str
   :delegation #{+CovenantId+}})


(defschema +PlatformUser+
  {:name +UserName+
   :password s/Str
   :queues +Queues+
   :exchange s/Str
   :allocations +Allocations+
   (s/optional-key :localusers) {+UserName+ +LocalUser+}
   (s/optional-key :delegation) {+UserName+ #{+CovenantId+}}
   (s/optional-key :transparent-delegation) {+CovenantId+ +CovenantId+}
   ;(s/optional-key :transparent-proxy) #{+CovenantId+}
   })

(defschema +Covenant+
  {:from +UserName+
   :to +UserName+
   :tag s/Str})


(defschema +Contracts+
  {:users {+UserName+ +PlatformUser+}
   :covenants {+CovenantId+ +Covenant+}
   :collections {+CovenantCollectionId+ #{+CovenantId+}}})
;;;;;;;;;;;;;; Schema for Contracts via REST ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Swagger can't handle schemas with arbitrary keys, so we can't use those 
;; instead use many arrays
(defschema +Contracts-REST+
  {:users [{:name +UserName+
            :password s/Str
            :exchange s/Str
            :queues [s/Str]
            :allocations [{:queues [s/Str]
                           :covenant +CovenantId+}]
            (s/optional-key :localusers) [+LocalUser+]
            (s/optional-key :delegation) [{:to-user +UserName+
                                           :covenants #{+CovenantId+}}] 
            (s/optional-key :forwardings) [{:source-covenant +CovenantId+
                                                 :target-covenant +CovenantId+}]}]
   :covenants [(assoc +Covenant+ :id +CovenantId+)]
   :collections [{:name s/Str
                  :covenants [+CovenantId+]}]})

(defn external->internal-contracts 
  "Convert submitted data structure into our internal format."
  [{:keys [users covenants collections]}]
  {:users (into {} (for [{:keys [name password queues exchange allocations localusers delegation forwardings]} users] 
            [name
             {:name name 
              :password password 
              :queues (set queues) 
              :exchange exchange
              :allocations (into {} (for [{qs :queues c :covenant} allocations] [c (set qs)]))
              :localusers (reduce #(assoc % (:name %2) %2) {} localusers)
              :transparent-delegation (into {} (for [{s :source-covenant t :target-covenant} forwardings] [s t]))
              :delegation (into {} (for [{t :to-user c :covenants} delegation] [t (set c)]))}])) 
   :covenants (into {} (for [c covenants]
                         [(:id c) (dissoc c :id)]))
   :collections (reduce (fn [m c] (assoc m (:name c) (set (:covenants c)))) {} collections)})

(defn internal->external-contracts 
  "Convert internal contracts data structure into a format that is more suitable for Swagger/REST clients."
  [{:keys [users covenants collections]}]
  {:users (for [{:keys [name password queues exchange allocations localusers delegation transparent-delegation]} (vals users)] 
            {:name name 
             :password password 
             :queues (vec queues) 
             :exchange exchange
             :allocations (for [[id qs] allocations] {:queues (vec qs) :covenant id})
             :localusers (vals localusers)
             :forwardings (for [[f t] transparent-delegation] {:source-covenant f :target-covenant t})
             :delegation (for [[n covs] delegation] {:to-user n :covenants (vec covs)})})
   :covenants (for [[id c] covenants] (assoc c :id id))
   :collections (for [[n ids] collections] {:name n :covenants (vec ids)})})

;;;;;;;;;;;;;; Schema Definitions for Credentials ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defschema +Credentials+
  "Schema for credentials for rabbitmq's management api and other credentials for a cluster
with at least one node
- `:name` is the main name of this credentials set/the host this gets applied to
- `:aliases` may be aliases for `name`"
  {:cluster-name s/Str
   :node-urls [s/Str]
   :ppu-vhost s/Str
   :management {:user +UserName+
                :password s/Str}
   (s/optional-key :shovel) {:user +UserName+
                              :password s/Str
                              :password-hash s/Str}
   (s/optional-key :aliases) [s/Str]})