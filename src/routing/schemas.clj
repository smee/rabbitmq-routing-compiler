(ns ^{:doc "Type annotations for core data structures. Uses Prismatic's schema library."}
routing.schemas
  (:require [schema.core :as s]))

;;;;;;;;;;;;;; Schema Definitions for Contracts ;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def +UserName+ s/Str)
(def +CovenantId+ s/Str)
(def +CovenantCollectionId+ s/Str)

(def +Queues+ #{s/Str})

(def +Allocations+ {s/Str #{s/Str}})

(def +LocalUser+ {:name +UserName+
                  :password s/Str
                  :queues +Queues+
                  :exchange s/Str
                  :delegation #{+CovenantId+}})


(def +PlatformUser+ {:name +UserName+
                     :password s/Str
                     :queues +Queues+
                     :exchange s/Str
                     :allocations +Allocations+
                     ;a user may represent multiple upstream users (à la transparent proxy)
                     (s/optional-key :localusers) {+UserName+ +LocalUser+}
                     (s/optional-key :delegation) {+UserName+ #{+CovenantId+}}
                     (s/optional-key :transparent-delegation) {+CovenantId+ +CovenantId+}}) 

(def +Covenant+ {:from s/Str
                 :to s/Str
                 :tag s/Str})


(def +Contracts+ {:users {+UserName+ +PlatformUser+}
                  :covenants {+CovenantId+ +Covenant+}
                  :collections {+CovenantCollectionId+ #{+CovenantId+}}})

;;;;;;;;;;;;;; Schema Definitions for Credentials ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def +Credentials+ 
  "Schema for credentials for rabbitmq's management api and other credentials for a cluster
with at least one node
- `:name` is the main name of this credentials set/the host this gets applied to
- `:aliases` may be aliases for `name`" 
  {:cluster-name s/Str
   :node-urls [(s/one s/Str "main-url") s/Str]
   :ppu-vhost s/Str
   :management {:user s/Str
                :password s/Str}
   (s/optional-key :shovel) {:user s/Str
                              :password s/Str
                              :password-hash s/Str} 
   (s/optional-key :aliases) [s/Str]})