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
                     ;a user may represent manalysistiple upstream users (à la transparent proxy)
                     (s/optional-key :remote) {:aliases #{+UserName+}
                                               :uri s/Str
                                               :local-uri s/Str
                                               :exchange s/Str
                                               :queue s/Str} 
                     (s/optional-key :localusers) {+UserName+ +LocalUser+}
                     (s/optional-key :delegation) {+UserName+ #{+CovenantId+}}}) 

(def +Covenant+ {:from s/Str
                 :to s/Str
                 :tag s/Str})


(def +Contracts+ {:users {+UserName+ +PlatformUser+}
                  :covenants {+CovenantId+ +Covenant+}
                  :collections {+CovenantCollectionId+ #{+CovenantId+}}})

;;;;;;;;;;;;;; Schema Definitions for Credentials ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def +Credentials+ 
  "Schema for credentials for rabbitmq's management api and other credentials.
- `:name` is the main name of this credentials set/the host this gets applied to
- `:aliases` may be aliases for `name`" 
  {:ppu-vhost s/Str
   :management {:user s/Str
                :password s/Str
                :url s/Str}
   (s/optional-key :shovel) {:user s/Str
                             :password s/Str
                             :password-hash s/Str} 
   (s/optional-key :name) s/Str
   (s/optional-key :aliases) [s/Str]})