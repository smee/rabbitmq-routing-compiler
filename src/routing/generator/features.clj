(ns ^{:doc "Constructors for distinct routing features in RabbitMQ. These
features represent structures that should be present within the messaging broker
for this feature to be regarded as active."}
routing.generator.features
  (:require [clojure 
             [set :as cs]
             [string :refer [split join]]
             [walk :as w]]
            schema.core
            [routing.schemas :refer [+Credentials+ +Contracts+]]
            [routing.generator
             [common :refer [as-flat-set]] 
             [rabbit-password :refer [rabbit-password-hash]]])
  (:import clojure.lang.IFn))

(defmacro deffeature 
  "Every single feature takes exactly three arguments:
    - `contracts` matching routing.schema/+Contracts+
    - `credentials` matching routing.schema/+Credentials+
    - `vhost-of` a function taking the user name and returning the name of the vhost for this user's structures in RabbitMQ.
The body of this feature may return an arbitrarily nested structure of sequences where the leafs
must be maps that `routing.generator.io` knows to handle.
The returned value is a set of all distinct maps of the `body`."
  [fn-name doc-string [contracts credentials vhost-of] & body]
  `(schema.core/defn ~fn-name
     ~doc-string
     [~contracts :- +Contracts+
      ~credentials :- +Credentials+
      ~vhost-of :- IFn]
     (as-flat-set ~@body)))


(def ^:const invalid_routing_key "admin.unroutable.exchange")

(defn user-exchange-read [name]
  (str name "-ex-read"))

(defn user-exchange-write-internal [name]
  (str name "-internal"))

(defn generated-vhost? [vhost]
  (.startsWith vhost "VH_"))

;;;;;;;;;;;;;;; common declaration, might be used by multiple generators ;;;;;;;;;;;;;;;;;;;;;;
(defn- generate-invalid-routing-exchange 
  "In each vhost we use the argument `alternative-exchange`
to route message that would otherwise get dropped because
there is no outgoing binding matching the routing key of a message."
  [vhost]
  [{:resource :exchange 
   :vhost vhost
   :arguments {:name invalid_routing_key 
               :type "fanout" 
               :internal false 
               :durable true 
               :auto_delete false 
               :arguments {}}}
   {:resource :exchange 
   :vhost vhost
   :arguments {:name "admin.dropped" 
               :type "fanout" 
               :internal false 
               :durable true 
               :auto_delete false 
               :arguments {}}}])

(defn- generate-private-resources-for [user exchange vhost]
  [{:resource :exchange 
    :vhost vhost
    :arguments {:name exchange 
                :type "topic" 
                :internal false 
                :durable true 
                :auto_delete false 
                :arguments {:alternate-exchange invalid_routing_key}}}
   {:resource :exchange 
      :vhost vhost
      :arguments {:name (user-exchange-write-internal user)  
                  :type "topic" 
                  :internal true 
                  :durable true 
                  :auto_delete false 
                  :arguments {:alternate-exchange invalid_routing_key}}}
   {:resource :exchange 
    :vhost vhost
    :arguments {:name (user-exchange-read user) 
                :type "topic" 
                :internal true 
                :durable true 
                :auto_delete false 
                :arguments {:alternate-exchange invalid_routing_key}}}])

(deffeature construct-admin-declarations 
  "Create ppu vhost, grant all permissions to the management-user for all generated vhosts and the ppu vhost."
  [{:keys [users]} 
   {{admin :user} :management vhost :ppu-vhost} 
   vhost-of]
  {:resource :vhost 
   :name vhost}
  {:resource :permission 
   :vhost vhost 
   :user admin 
   :configure ".*" 
   :write ".*" 
   :read ".*"}
  (for [user (keys users)] ;admin has all rights in user's vhosts
    {:resource :permission 
     :vhost (vhost-of user) 
     :user admin 
     :configure ".*" 
     :write ".*" 
     :read ".*"}))

(deffeature construct-admin-permissions-for-vhosts 
  "Create ppu vhost, grant all permissions to the management-user for all generated vhosts and the ppu vhost."
  [{:keys [users]} creds vhost-of]
  (for [user (keys users)] ;admin has all rights in user's vhosts
    {:resource :permission 
     :vhost (vhost-of user) 
     :user (-> creds :management :user) 
     :configure ".*" 
     :write ".*" 
     :read ".*"}))

(deffeature construct-routing-key-only 
  "Construct routing that uses only routing keys, no header arguments at all.
All routing keys have the following structure:
    SENDER_ID.tag.COVENANTCOLLECTION"
  [{:keys [users covenants collections]} {vhost :ppu-vhost} vhost-of]
  (generate-invalid-routing-exchange vhost)
  (for [{user :name ex :exchange} (vals users)] 
    [(generate-private-resources-for user ex vhost)
     {:resource :exchange-binding 
      :vhost vhost
      :from ex 
      :to (user-exchange-write-internal user)
      :arguments {:routing_key (str user ".#") 
                  :arguments {}}}
     ; iterate all collections, create bindings for each access right
     (for [[ccollection-id cov-ids] collections, 
           cov-id cov-ids
           :let [{:keys [from to tag]} (get covenants cov-id)]
           :when (and (contains? users from) (contains? users to))]   
       {:resource :exchange-binding 
        :vhost vhost
        :from (user-exchange-write-internal from) 
        :to (user-exchange-read to)
        :arguments {:routing_key (format "*.%s.%s" tag ccollection-id) 
                    :arguments {}}})]))

;;;; constructors for private vhost per user
(deffeature construct-user-vhosts 
  "Each user has a private vhost named like the user."
  [{:keys [users]} _ vhost-of]
  (for [user (keys users)]
    {:resource :vhost
     :name (vhost-of user)}))

(deffeature construct-users 
  "Every generated user has its name as a password and a tag 'generated'."
  [{:keys [users]} _ vhost-of]
  (for [{user :name pw :password} (vals users)] 
    {:resource :user
     :name user 
     :password_hash pw 
     :tags "generated"}))

(deffeature construct-internal-shovel-user
  "TODO"
  [{:keys [users]} {:keys [shovel ppu-vhost]} vhost-of] 
  {:resource :user
   :name (shovel :user) 
   :password_hash (shovel :password-hash) 
   :tags "generated"}
  (for [vhost (cons ppu-vhost (map vhost-of (keys users)))] 
    {:resource :permission 
     :vhost vhost
     :user (shovel :user)
     :configure ".*";FIXME should not be necessary, why does the shovel plugin need to do declarations??? 
     :write ".*" 
     :read ".*"}))

(defn- escape-rabbitmq-regex 
  [^String s]
  (-> s
    (.replace "." "\\.")
    (.replace "*" "\\*")
    (.replace "|" "\\|")
    (.replace "^" "\\^")
    (.replace "$" "\\$")))

(deffeature construct-permissions 
  "Each user only has only read permissions to his queues and write permission to his own exchange.
Users have no permissions to change anything themselves."
  [{:keys [users queues]} _ vhost-of]
  (for [{user :name ex :exchange qs :queues} (vals users)] 
    {:resource :permission 
     :vhost (vhost-of user)
     :user user
     :configure "^$" 
     :write (escape-rabbitmq-regex ex)
     :read (join "|" (map escape-rabbitmq-regex qs))}))


(deffeature construct-private-queue-bindings 
  "Declare queues and bindings according to `allocations`."
  [{:keys [users covenants]} _ vhost-of]
  (for [[user {:keys [queues allocations exchange]}] users 
        :let [vh (vhost-of user)]] 
    [(generate-private-resources-for user exchange vh)
     (generate-invalid-routing-exchange vh)
     (for [[c-id queues] allocations, queue queues
           :let [{:keys [from tag]} (get covenants c-id)
                 uerp (user-exchange-read user)]]  
       {:resource :queue-binding 
        :vhost vh
        :to queue 
        :from uerp
        :arguments {:routing_key (format "%s.%s.*" from tag) 
                    :arguments {}}})
     (for [queue queues] 
       {:resource :queue
        :vhost vh
        :arguments {:name queue 
                    :durable true 
                    :auto_delete false 
                    :arguments {:x-dead-letter-exchange "admin.dropped"}}})])) ; TODO x-dead-letter-exchange, see https://www.rabbitmq.com/dlx.html

(deffeature construct-internal-federations 
  "Federation based alternative of construct-internal-shovels"
  [contracts {mgmt :management, ppu-vhost :ppu-vhost} vhost-of]
  (for [{user :name ex :exchange} (vals (:users contracts)) 
        :let [vh (vhost-of user)
              uup (str "gen-" vh "-up") 
              udo (str "gen-" vh "-down")
              uups (str uup "-set")
              udos (str udo "-set")
              ex-r (user-exchange-read user)
              ex-w ex]]
    [{:resource :federation-upstream 
      :vhost vh
      :name uup 
      :uri (format "amqp://%s:%s@/%s" (mgmt :user) (mgmt :password) ppu-vhost)}
     {:resource :federation-upstream 
      :vhost ppu-vhost
      :name udo 
      :uri (format "amqp://%s:%s@/%s" (mgmt :user) (mgmt :password) vh)}
     {:resource :federation-upstream-set
      :vhost vh
      :name uups
      :upstream uup
      :exchange ex-r}
     {:resource :federation-upstream-set
      :vhost ppu-vhost
      :name udos
      :upstream udo
      :exchange ex-w}
     {:resource :federation-policy
      :vhost ppu-vhost
      :federation-upstream-set udos
      :name (str udos "-policy")
      :pattern (str "^" ex-w "$")}
     {:resource :federation-policy
      :federation-upstream-set uups
      :name (str uups "-policy") 
      :vhost vh
      :pattern (str "^" ex-r "$")}]))

(deffeature construct-internal-shovels 
  "TODO" 
  [contracts {shvl :shovel, ppu-vhost :ppu-vhost} vhost-of]
  (for [{user :name ex :exchange} (vals (:users contracts)) 
        :let [vh (vhost-of user)
              ex-r (user-exchange-read user)
              ex-w ex
              ex-w-queue (str ex-w "_Q")
              ex-r-queue (str ex-r "_Q")
              {shovel-user :user shovel-password :password} shvl]]
    [{:resource :shovel
      :vhost vh
      :name (str ppu-vhost "->" vh) 
      :src-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password ppu-vhost)
      :src-queue ex-r-queue
      :dest-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password vh)
      :dest-exchange ex-r 
      :prefetch-count 100
      :reconnect-delay 1
      :add-forward-headers false 
      :ack-mode "on-publish"}
     {:resource :shovel
      :vhost vh
      :name (str vh "->" ppu-vhost)
      :src-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password vh)
      :src-queue ex-w-queue
      :dest-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password ppu-vhost)
      :dest-exchange ex-w
      :prefetch-count 100
      :reconnect-delay 1
      :add-forward-headers false 
      :ack-mode "on-publish"}
     {:resource :queue
      :vhost vh
      :arguments {:name ex-w-queue 
                  :durable true 
                  :auto_delete false 
                  :arguments {}}}
     {:resource :queue
      :vhost ppu-vhost
      :arguments {:name ex-r-queue 
                  :durable true 
                  :auto_delete false 
                  :arguments {}}}
     {:resource :queue-binding 
      :vhost vh
      :to ex-w-queue 
      :from ex-w 
      :arguments {:routing_key "#" :arguments {}}}
     {:resource :queue-binding 
      :vhost ppu-vhost
      :to ex-r-queue 
      :from ex-r 
      :arguments {:routing_key "#" :arguments {}}}]))





#_(deffeature construct-delegation-routing 
  "Fully transparent elegation of covenants between platform users using routing-key rewriting shovels."
  [{:keys [users collections covenants] :as contracts} {ppu-vhost :ppu-vhost} vhost-of]
  (for [[user-name {ds :delegation :as user}] users,
        [delegating-user cov-ids] ds,
        cov-id cov-ids
        :when cov-id
        :let [vh (vhost-of user-name)
              ex-w (:exchange user)
              {:keys [from to tag]} (get covenants cov-id)]]
    )) 

;;;;;;;;;;;;;;;; Tracing, Poor mans auditing.... ;;;;;;;;;;;;;;;;
(defn- tracing-in-vhost [queue-name vhost]
  #{{:resource :tracing 
     :vhost vhost}
    {:resource :queue
     :vhost vhost
     :arguments {:name queue-name 
                 :durable true 
                 :auto_delete false 
                 :arguments {:x-message-ttl 30000
                             :x-max-length 100}}}
    {:resource :queue-binding 
     :vhost vhost
     :to queue-name
     :from "amq.rabbitmq.trace" 
     :arguments {:routing_key "#"  :arguments {}}}})

(def ^:private ^:const tracing-queue "admin.tracing.queue")

(deffeature construct-tracing 
  "Enable tracing in a vhost, construct and add a queue to the amq.rabbitmq.trace exchange.
Queue keeps messages for 30s, holds max. 100 messages (to avoid making this feature a bottleneck)"
  [{:keys [users]} {ppu-vhost :ppu-vhost} vhost-of]
  (->> users
    keys
    (map vhost-of)
    (cons ppu-vhost)
    (map (partial tracing-in-vhost tracing-queue))
    (reduce into)))

;;;;;;;;;;;;;;;;;;; Handle unroutable messages ;;;;;;;;;;;;;;;;;
(def ^:private ^:const unroutable-queue "admin.unroutable.queue") 
(deffeature construct-unroutable 
  "Unroutable messages go via the internal exchange `invalid_routing_key` to a new queue,
then get shoveled to `invalid_routing_key` in the ppu vhost (with forward headers so we know
where the message was stuck)."
  [_ {:keys [ppu-vhost]} _]
  {:resource :queue
   :vhost ppu-vhost 
   :arguments {:name unroutable-queue 
               :durable true 
               :auto_delete false 
               :arguments {:x-message-ttl (* 1000 60 60 24) ;save for max. 24 hours 
                           :x-max-length 100}}}
  {:resource :queue-binding 
   :vhost ppu-vhost
   :to unroutable-queue 
   :from invalid_routing_key 
   :arguments {:routing_key "#" :arguments {}}}) 

(deffeature construct-unroutable-separate-vhosts 
  "Unroutable messages go via the internal exchange `invalid_routing_key` to a new queue,
then get shoveled to `invalid_routing_key` in the ppu vhost (with forward headers so we know
where the message was stuck)."
  [{:keys [users] :as contracts} {{:keys [ppu-vhost shovel-user shovel-password]} :shovel :as credentials} vhost-of]
  (for [user (keys users)
        :let [vhost (vhost-of user)]] 
    [{:resource :queue
      :vhost vhost 
      :arguments {:name unroutable-queue 
                  :durable true 
                  :auto_delete false 
                  :arguments {:x-message-ttl (* 1000 30) ;save for max. 30s 
                              :x-max-length 100}}};at most save the last 100 messages
     {:resource :queue-binding 
      :vhost vhost
      :to unroutable-queue 
      :from invalid_routing_key 
      :arguments {:routing_key "#" :arguments {}}}
     {:resource :shovel
      :vhost vhost 
      :name (str "unroutable in " vhost) 
      :src-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password vhost)
      :src-queue unroutable-queue
      :dest-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password ppu-vhost)
      :dest-exchange invalid_routing_key 
      :prefetch-count 100
      :reconnect-delay 1
      :add-forward-headers true 
      :ack-mode "on-publish"}]) 
  ; add queue for all non-routable messages in vhost `ppu-vhost`
  (construct-unroutable contracts credentials vhost-of))

(deffeature construct-high-availability-for-queues
  "Ensure that all queues get replicated to all cluster nodes"
  [_ {vh :ppu-vhost} _]
  {:resource :policy
   :name "ha-all-queues"
   :vhost vh
   :pattern ""
   :definition {:ha-mode "all"}
   :apply-to "queues"})

;;;;;;;;;;;;;;; update of clusters ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deffeature construct-big-bang-migrations 
  "Federation based cluster update to migrate users from one cluster to a new one."
  [contracts {vhost :ppu-vhost} _]
  (let [uup (str "gen-" vhost "-up") 
        uups (str uup "-set")
        exchanges (clojure.string/join "|" (->> contracts :users vals (map :exchange)))]
    [{:resource :federation-upstream 
      :vhost vhost
      :name uup 
      :uri "amqp://guest:guest@localhost:5673/VH_ppu"} ; FIXME make this a parameter
     {:resource :federation-policy
      :vhost vhost
      :federation-upstream-set "all"
      :name (str uups "-policy")
      :apply-to "exchanges"
      :pattern (str "^" exchanges "$")}]))

