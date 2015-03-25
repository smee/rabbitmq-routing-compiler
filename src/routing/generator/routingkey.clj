(ns routing.generator.routingkey
  (:require [clojure 
             [set :as cs]
             [string :refer [split join]]
             [walk :as w]]
            [routing.generator.common :refer [as-flat-set]] 
            [routing.generator.rabbit-password :refer [rabbit-password-hash]]))

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
  {:action :declare 
   :resource :exchange 
   :vhost vhost
   :arguments {:name invalid_routing_key 
               :type "fanout" 
               :internal false 
               :durable true 
               :auto_delete false 
               :arguments {}}})

(defn- generate-private-resources-for [user exchange vhost]
  [{:action :declare 
    :resource :exchange 
    :vhost vhost
    :arguments {:name exchange 
                :type "topic" 
                :internal false 
                :durable true 
                :auto_delete false 
                :arguments {:alternate-exchange invalid_routing_key}}}
   {:action :declare 
    :resource :exchange 
    :vhost vhost
    :arguments {:name (user-exchange-read user) 
                :type "topic" 
                :internal false 
                :durable true 
                :auto_delete false 
                :arguments {:alternate-exchange invalid_routing_key}}}])

(defn construct-admin-declarations 
  "Create ppu vhost, grant all permissions to the management-user for all generated vhosts and the ppu vhost."
  [{:keys [users]} {:keys [management-user management-password ppu-vhost]} vhost-of]
  (as-flat-set
    {:action :declare 
     :resource :vhost 
     :name ppu-vhost}
    {:action :declare 
     :resource :permission 
     :vhost ppu-vhost 
     :user management-user 
     :configure ".*" 
     :write ".*" 
     :read ".*"}
    (for [user (keys users)] ;admin has all rights in user's vhosts
      {:action :declare 
       :resource :permission 
       :vhost (vhost-of user) 
       :user management-user 
       :configure ".*" 
       :write ".*" 
       :read ".*"})))

(defn construct-admin-permissions-for-vhosts 
  "Create ppu vhost, grant all permissions to the management-user for all generated vhosts and the ppu vhost."
  [{:keys [users]} {:keys [management-user management-password ppu-vhost]} vhost-of]
  (as-flat-set
    (for [user (keys users)] ;admin has all rights in user's vhosts
      {:action :declare 
       :resource :permission 
       :vhost (vhost-of user) 
       :user management-user 
       :configure ".*" 
       :write ".*" 
       :read ".*"})))

(defn construct-routing-key-only 
  "Construct routing that uses only routing keys, no header arguments at all.
All routing keys have the following structure:
    SENDER_ID.tag.COVENANTCOLLECTION"
  [{:keys [users covenants collections]} {vhost :ppu-vhost} vhost-of]
  (as-flat-set 
    (generate-invalid-routing-exchange vhost)
    (for [{user :name ex :exchange} (vals users)] 
      [(generate-private-resources-for user ex vhost)
       {:action :declare 
        :resource :exchange 
        :vhost vhost
        :arguments {:name (user-exchange-write-internal user)  
                    :type "topic" 
                    :internal true 
                    :durable true 
                    :auto_delete false 
                    :arguments {:alternate-exchange invalid_routing_key}}}
       {:action :bind 
        :resource :exchange 
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
       {:action :bind 
        :resource :exchange 
        :vhost vhost
        :from (user-exchange-write-internal from) 
        :to (user-exchange-read to) 
        :arguments {:routing_key (format "*.%s.%s" tag ccollection-id) 
                    :arguments {}}})])))

;;;; constructors for private vhost per user
(defn construct-user-vhosts 
  "Each user has a private vhost named like the user."
  [{:keys [users]} _ vhost-of]
  (as-flat-set 
    (for [user (keys users)]
      {:action :declare 
       :resource :vhost
       :name (vhost-of user)})))

(defn construct-users 
  "Every generated user has its name as a password and a tag 'generated'."
  [{:keys [users]} _ vhost-of]
  (as-flat-set 
    (for [{user :name pw :password} (vals users)] 
      {:action :declare 
       :resource :user
       :name user 
       :password_hash pw 
       :tags "generated"})))

(defn construct-internal-shovel-user [{:keys [users]} {:keys [shovel-user shovel-password-hash ppu-vhost]} vhost-of]
  (as-flat-set 
    {:action :declare 
     :resource :user
     :name shovel-user 
     :password_hash shovel-password-hash 
     :tags "generated"}
    (for [vhost (cons ppu-vhost (map vhost-of (keys users)))] 
      {:action :declare 
       :resource :permission 
       :vhost vhost
       :user shovel-user
       :configure ".*";FIXME should not be necessary, why does the shovel plugin need to do declarations??? 
       :write ".*" 
       :read ".*"})))

(defn- escape-rabbitmq-regex 
  [^String s]
  (-> s
    (.replace "." "\\.")
    (.replace "*" "\\*")
    (.replace "|" "\\|")
    (.replace "^" "\\^")
    (.replace "$" "\\$")))

(defn construct-permissions 
  "Each user only has only read permissions to his queues and write permission to his own exchange.
Users have no permissions to change anything themselves."
  [{:keys [users queues]} _ vhost-of]
  (as-flat-set 
    (for [{user :name ex :exchange qs :queues} (vals users)] 
      {:action :declare 
       :resource :permission 
       :vhost (vhost-of user)
       :user user
       :configure "^$" 
       :write (escape-rabbitmq-regex ex)
       :read (join "|" (map escape-rabbitmq-regex qs))})))


(defn construct-private-queue-bindings 
  "Declare queues and bindings according to `allocations`."
  [{:keys [users covenants]} _ vhost-of]
  (as-flat-set 
    (for [[user {:keys [queues allocations exchange]}] users 
          :let [vh (vhost-of user)]] 
      [(generate-private-resources-for user exchange vh)
       (generate-invalid-routing-exchange vh)
       (for [[c-id queues] allocations, queue queues
             :let [{:keys [from tag]} (get covenants c-id)
                   uerp (user-exchange-read user)]]  
         {:action :bind 
          :resource :queue 
          :vhost vh
          :to queue 
          :from uerp 
          :arguments {:routing_key (format "%s.%s.*" from tag) 
                      :arguments {}}})
       (for [queue queues] 
         {:action :declare 
          :resource :queue
          :vhost vh
          :arguments {:name queue 
                      :durable true 
                      :auto_delete false 
                      :arguments {}}})]))) ; TODO x-dead-letter-exchange, see https://www.rabbitmq.com/dlx.html

(defn construct-federations 
  "Federation based alternative of construct-internal-shovels"
  [contracts {:keys [management-user management-password ppu-vhost]} vhost-of]
  (as-flat-set 
    (for [{user :name ex :exchange} (vals (:users contracts)) 
          :let [vh (vhost-of user)
                uup (str "gen-" vh "-up") 
                udo (str "gen-" vh "-down")
                uups (str uup "-set")
                udos (str udo "-set")
                ex-r (user-exchange-read user)
                ex-w ex]]
      [{:resource :federation-upstream 
        :action :declare 
        :vhost vh
        :name uup 
        :uri (format "amqp://%s:%s@/%s" management-user management-password ppu-vhost)}
       {:resource :federation-upstream 
        :action :declare 
        :vhost ppu-vhost
        :name udo 
        :uri (format "amqp://%s:%s@/%s" management-user management-password vh)}
       {:resource :federation-upstream-set
        :action :declare
        :vhost vh
        :name uups
        :upstream uup
        :exchange ex-r}
       {:resource :federation-upstream-set
        :action :declare
        :vhost ppu-vhost
        :name udos
        :upstream udo
        :exchange ex-w}
       {:resource :federation-policy
        :action :declare
        :vhost ppu-vhost
        :federation-upstream-set udos
        :name (str udos "-policy")
        :pattern (str "^" ex-w "$")}
       {:resource :federation-policy
        :action :declare
        :federation-upstream-set uups
        :name (str uups "-policy") 
        :vhost vh
        :pattern (str "^" ex-r "$")}])))

(defn construct-internal-shovels 
  [contracts {:keys [shovel-user shovel-password ppu-vhost]} vhost-of]
  (as-flat-set 
    (for [{user :name ex :exchange} (vals (:users contracts)) 
          :let [vh (vhost-of user)
                ex-r (user-exchange-read user)
                ex-w ex
                ex-w-queue (str ex-w "_Q")
                ex-r-queue (str ex-r "_Q")]]
      [{:resource :shovel
        :action :declare
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
        :action :declare
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
       {:action :declare 
        :resource :queue
        :vhost vh
        :arguments {:name ex-w-queue 
                    :durable true 
                    :auto_delete false 
                    :arguments {}}}
       {:action :declare 
        :resource :queue
        :vhost ppu-vhost
        :arguments {:name ex-r-queue 
                    :durable true 
                    :auto_delete false 
                    :arguments {}}}
       {:action :bind 
        :resource :queue 
        :vhost vh
        :to ex-w-queue 
        :from ex-w 
        :arguments {:routing_key "#" :arguments {}}}
       {:action :bind 
        :resource :queue 
        :vhost ppu-vhost
        :to ex-r-queue 
        :from ex-r 
        :arguments {:routing_key "#" :arguments {}}}])))

;;;;;;;;;;;;;;;;; Upstream RabbitMQ Instances - Alias for remote users ;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- user-alias 
  "If a username is represented by some proxy user
  this function finds the correct name. If there is a platform or local user
  with the name `user-name`, `user-name` will get returned."
  [{users :users} user-name]
  (cond 
    (users user-name) user-name
    ((set (mapcat (comp (partial map :name) vals :localusers) (vals users))) user-name) user-name
    :else-must-be-remote (some #(when (get-in % [:remote :aliases user-name]) (:name %)) (vals users))))

(defn construct-alias-routing
  "Construct routing for all transparent remote users"
  [{:keys [users covenants collections] :as contracts} {ppu-vhost :ppu-vhost} vhost-of]
  (as-flat-set 
    (for [{remote-user :name, ex :exchange, queues :queues, {aliases :aliases :as remote} :remote} (vals users), 
          alias-user aliases
          :when alias-user
          :let [vhost (vhost-of remote-user)]] 
      [; shovel TO the remote rabbitmq
       {:resource :shovel
        :action :declare
        :vhost vhost
        :name (str vhost "-> remote" )
        :src-uri (:local-uri remote) ;we NEED valid user credentials for the local mom, too!
        :src-queue (first queues);; FIXME are we sure there is just one? 
        :dest-uri (:uri remote) 
        :dest-exchange (:exchange remote) 
        :prefetch-count 100
        :reconnect-delay 1
        :add-forward-headers false 
        :ack-mode "on-publish"}
       ; shovel FROM the remote rabbitmq
       {:resource :shovel
        :action :declare
        :vhost vhost
        :name (str  "remote ->" vhost)
        :src-uri (:uri remote)
        :src-queue (:queue remote) 
        :dest-uri (:local-uri remote) ;we NEED valid user credentials for the local mom, too!  
        :dest-exchange ex 
        :prefetch-count 100
        :reconnect-delay 1
        :add-forward-headers false 
        :ack-mode "on-publish"}
       ; bindings within ppu-vhost from remote users via this proxy user
       (for [[ccollection-id cov-ids] collections, 
             cov-id cov-ids
             :let [{:keys [from to tag]} (get covenants cov-id)]
             :when (contains? aliases from)
             :let [from-remote remote-user]]  
         [{:action :bind 
           :resource :exchange 
           :vhost ppu-vhost
           :from ex;(user-exchange-write-internal from-remote) 
           :to (user-exchange-read to)
           ; we do not know which covenant collection might have been defined upstream.
           ; but it doesn't matter: at this point we already know that the message is for use
           ; no point in further filtering recipients
           :arguments {:routing_key (format "%s.%s.*" from tag) :arguments {}}}])
       ; or from platform users via this proxy user to remote users
       (for [[ccollection-id cov-ids] collections, 
             cov-id cov-ids
             :let [{:keys [from to tag]} (get covenants cov-id)]
             :when (contains? aliases to)
             :let [to remote-user]]  
         {:action :bind 
          :resource :exchange 
          :vhost ppu-vhost
          :from (user-exchange-write-internal from) 
          :to (user-exchange-read to)
          :arguments {:routing_key (format "*.%s.%s" tag ccollection-id) 
                      :arguments {}}})])))

;;;;;;;;;;;;;;; Local users ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- find-pf-user-for-local [users local-user-name]
  (some (fn [[_ pf-user]]
          (when (get-in pf-user [:localusers local-user-name])
            pf-user)) users))

(defn construct-localuser-covenants 
  "localuser may have convenant with another local user. In this case (assuming they are both
localusers of the same platform user), we can directly bind from the sender's exchange
to the allocated queue."
  [{:keys [users collections covenants]} _ vhost-of]  
  (let [locals-per-pf-user (map (comp set keys :localusers) (vals users))] 
    (as-flat-set
      (for [[cov-id {:keys [from to tag]}] covenants 
            :when (some #(and (% from) (% to)) locals-per-pf-user)
            :let [pf-user (find-pf-user-for-local users to)
                  local-user-exchange (get-in pf-user [:localusers from :exchange])
                  queues (get-in pf-user [:allocations cov-id])]]
        (for [q queues]
          {:action :bind 
           :resource :queue 
           :vhost (vhost-of (:name pf-user)) 
           :from  local-user-exchange
           :to q 
           :arguments {:routing_key (format "%s.%s.*" from tag) :arguments {}}})))))

(defn construct-localusers 
  "Construct local users associated with a toplevel platform user. A platform user
may delegate covenants to its local users."
  [{:keys [users collections covenants]} _ vhost-of] 
  (as-flat-set
    (for [[pf-user {lu :localusers pf-user-exchange :exchange}] users,
          {:keys [name password exchange queues delegation]} (vals lu),
          :let [vh (vhost-of pf-user)]]
      
      [; create local user
       {:action :declare 
        :resource :user 
        :name name 
        :password_hash password 
        :tags "generated"}
       ; localuser has his own writable exchange
       {:action :declare 
        :resource :exchange
        :vhost vh
        :arguments {:name exchange 
                    :type "topic" 
                    :internal false 
                    :durable true 
                    :auto_delete false 
                    :arguments {:alternate-exchange invalid_routing_key}}}
       ; localuser may write to his exchange, read specific queues
       {:action :declare 
        :resource :permission 
        :vhost vh
        :user name
        :configure "^$" 
        :write exchange
        :read (join "|" queues)}
       ; localuser has bindings from his private exchange to its platform user's exchange
       ; for every covenant in every covenant-collection he is allowed to use
       (for [cov-id delegation, 
             [cov-coll-name cov-coll-ids] collections
             :when (contains? cov-coll-ids cov-id)]
         {:action :bind 
          :resource :exchange 
          :vhost vh
          :from exchange 
          :to pf-user-exchange 
          :arguments {:routing_key (format "%s.%s.%s" pf-user (get-in covenants [cov-id :tag]) cov-coll-name) :arguments {}}})])))


;;;;;;;;;; Delegation ;;;;;;;;;;;;;;;;;;;;;;;

(defn construct-delegation-routing 
  "Delegation of covenants between local platform users."
  [{:keys [users collections covenants] :as contracts} {ppu-vhost :ppu-vhost} vhost-of]
  (as-flat-set 
    ;; TODO what about the delegating user? he gets all messages, too!
    (for [[user-name {ds :delegation :as user}] users,
          [delegating-user cov-ids] ds,
          cov-id cov-ids
          :when cov-id
          :let [vh (vhost-of user-name)
                ex-w (:exchange user)
                {:keys [from to tag]} (get covenants cov-id)]]
      [; bindings for delegated sending covenants
       (if (= from delegating-user) 
         (for [[cc-name cov-coll] collections 
               :when (contains? cov-coll cov-id)
               :let [to (user-alias contracts to)]] 
           {:action :bind 
            :resource :exchange 
            :vhost ppu-vhost
            :from (:exchange user) 
            :to (get-in users [delegating-user :exchange]) 
            :arguments {:routing_key (format "%s.%s.%s" delegating-user tag cc-name) 
                        :arguments {}}})
         ; else: we receive from this delegated covenant
         (for [[cc-name cov-coll] collections 
               :when (contains? cov-coll cov-id)
               :let [from' (user-alias contracts from)]] 
           ; if this is a proxy user, we don't know which covenant collection might have been used
           ; but since we got the message, we are the recipient. No need to restrict it further
           (if (not= from from') ;remote user
             {:action :bind 
              :resource :exchange 
              :vhost ppu-vhost
              :from (get-in users [from' :exchange]) ;(user-exchange-write-internal from') 
              :to (user-exchange-read user-name) 
              :arguments {:routing_key (format "%s.%s.*" from tag) 
                          :arguments {}}}
             {:action :bind 
              :resource :exchange 
              :vhost ppu-vhost
              :from (user-exchange-write-internal from') 
              :to (user-exchange-read user-name) 
              :arguments {:routing_key (format "*.%s.%s" tag cc-name) 
                          :arguments {}}})))])))

;;;;;;;;;;;;;;;; Tracing, Poor mans auditing.... ;;;;;;;;;;;;;;;;
(defn- tracing-in-vhost [queue-name vhost]
  #{{:resource :tracing 
     :action :declare 
     :vhost vhost}
    {:action :declare 
     :resource :queue
     :vhost vhost
     :arguments {:name queue-name 
                 :durable true 
                 :auto_delete false 
                 :arguments {:x-message-ttl 30000
                             :x-max-length 100}}}
    {:action :bind 
     :resource :queue 
     :vhost vhost
     :to queue-name
     :from "amq.rabbitmq.trace" 
     :arguments {:routing_key "#"  :arguments {}}}})

(def ^:private ^:const tracing-queue "admin.tracing.queue")

(defn construct-tracing 
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
(defn construct-unroutable 
  "Unroutable messages go via the internal exchange `invalid_routing_key` to a new queue,
then get shoveled to `invalid_routing_key` in the ppu vhost (with forward headers so we know
where the message was stuck)."
  [_ {:keys [ppu-vhost]} _]
  (as-flat-set 
    {:action :declare 
     :resource :queue
     :vhost ppu-vhost 
     :arguments {:name unroutable-queue 
                 :durable true 
                 :auto_delete false 
                 :arguments {:x-message-ttl (* 1000 60 60 24) ;save for max. 24 hours 
                             :x-max-length 100}}}
    {:action :bind 
     :resource :queue 
     :vhost ppu-vhost
     :to unroutable-queue 
     :from invalid_routing_key 
     :arguments {:routing_key "#" :arguments {}}})) 

(defn construct-unroutable-separate-vhosts 
  "Unroutable messages go via the internal exchange `invalid_routing_key` to a new queue,
then get shoveled to `invalid_routing_key` in the ppu vhost (with forward headers so we know
where the message was stuck)."
  [{:keys [users] :as contracts} {:keys [ppu-vhost shovel-user shovel-password] :as credentials} vhost-of]
  (as-flat-set 
    (for [user (keys users)
          :let [vhost (vhost-of user)]] 
      [{:action :declare 
        :resource :queue
        :vhost vhost 
        :arguments {:name unroutable-queue 
                    :durable true 
                    :auto_delete false 
                    :arguments {:x-message-ttl (* 1000 30) ;save for max. 30s 
                                :x-max-length 100}}};at most save the last 100 messages
       {:action :bind 
        :resource :queue 
        :vhost vhost
        :to unroutable-queue 
        :from invalid_routing_key 
        :arguments {:routing_key "#" :arguments {}}}
       {:resource :shovel
        :action :declare
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
    (construct-unroutable contracts credentials vhost-of)))