(ns routing.generator.features.delegation
  (:require [routing.generator.features :refer [deffeature user-exchange-read user-exchange-write-internal]]))

;;;;;;;;;; Delegation ;;;;;;;;;;;;;;;;;;;;;;;

(defn- user-alias 
  "If a username is represented by some proxy user
  this function finds the correct name. If there is a platform or local user
  with the name `user-name`, `user-name` will get returned."
  [{users :users} user-name]
  (cond 
    (users user-name) user-name
    ((set (mapcat (comp (partial map :name) vals :localusers) (vals users))) user-name) user-name))

(deffeature construct-delegation-routing 
  "Delegation of covenants between local and platform users"
  [{:keys [users collections covenants] :as contracts} 
   {ppu-vhost :ppu-vhost} 
   vhost-of]
  (for [[delegating-user {ds :delegation ex-w :exchange}] users,
        [delegating-to-user cov-ids] ds,
        cov-id-to-delegate cov-ids
        :let [{:keys [from to tag]} (get covenants cov-id-to-delegate)]]
    
    (if (= from delegating-user)
      ; bindings for delegated sending covenants
      [{:resource :exchange-binding 
        :vhost ppu-vhost
        :from (-> delegating-to-user users :exchange) 
        :to ex-w 
        :arguments {:routing_key (format "%s.%s.*" delegating-user tag) 
                    :arguments {}}}
       {:resource :exchange-binding 
        :vhost ppu-vhost
        :from (-> delegating-to-user users :exchange) 
        :to ex-w 
        :arguments {:routing_key (format "%s.%s" delegating-user tag) 
                    :arguments {}}}]
      ; bindings for delegated receiving convenants
    [{:resource :exchange-binding 
      :vhost ppu-vhost
      :from (user-exchange-write-internal from) 
      :to (user-exchange-read delegating-to-user) 
      :arguments {:routing_key (format "%s.%s.*" from tag) 
                  :arguments {}}}
     {:resource :exchange-binding 
      :vhost ppu-vhost
      :from (user-exchange-write-internal from) 
      :to (user-exchange-read delegating-to-user) 
      :arguments {:routing_key (format "%s.%s" from tag) 
                  :arguments {}}}])))

(deffeature construct-transparent-delegation-routing
  "Transparent delegation of covenants. Uses shovels to rename routing keys so
that a recipient of a delegation doesn't need to be aware of the fact that he
is a subcontractor."
  [{:keys [users collections covenants] :as contracts} 
   {{shovel-user :user shovel-password :password} :shovel} 
   vhost-of]
  (for [[delegating-user {ds :transparent-delegation ex-w :exchange}] users,
        [cov-from cov-to] ds,
        :let [{cf-from :from cf-to :to cf-tag :tag} (get covenants cov-from)
              {ct-from :from ct-to :to ct-tag :tag} (get covenants cov-to)
              vh-from (vhost-of cf-from)
              vh-to (vhost-of ct-to)
              ex-w-from (-> cf-from users :exchange)
              queue (format "delegation_'%s'->'%s'" cov-from cov-to)]]    
    
    [{:resource :queue
        :vhost vh-from
        :arguments {:name queue 
                    :durable true 
                    :auto_delete false 
                    :arguments {:x-dead-letter-exchange "admin.dropped"}}}
     {:resource :queue-binding 
        :vhost vh-from
        :to queue 
        :from ex-w-from
        :arguments {:routing_key (format "%s.%s.#" cf-from cf-tag) 
                    :arguments {}}}       
     {:resource :shovel
        :vhost vh-to
        :name queue 
        :src-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password vh-from)
        :src-queue queue
        :dest-uri (format "amqp://%s:%s@/%s" shovel-user shovel-password vh-to)
        :dest-exchange ex-w 
        :prefetch-count 100
        :reconnect-delay 1
        :add-forward-headers false 
        :ack-mode "on-publish"
        :dest-exchange-key (format "%s.%s" delegating-user ct-tag)}]))
