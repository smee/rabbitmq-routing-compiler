(ns routing.generator.features.remote-proxy
  (:require [routing.generator.features :as feat :refer [deffeature]]))

;;;;;;;;;;;;;;;;; Upstream RabbitMQ Instances - Alias for remote users ;;;;;;;;;;;;;;;;;;;;;;;;;


(deffeature construct-alias-routing
  "Construct routing for all transparent remote users"
  [{:keys [users covenants collections] :as contracts} {ppu-vhost :ppu-vhost} vhost-of]
  (for [{remote-user :name, ex :exchange, queues :queues, local-pw :password, {aliases :aliases :as remote} :remote} (vals users), 
        alias-user aliases
        :when alias-user
        :let [vhost (vhost-of remote-user)
              local-uri (format "amqp://%s:%s@/%s" remote-user local-pw vhost)]] 
    [; shovel TO the remote rabbitmq
     {:resource :shovel
      :vhost vhost
      :name (str vhost "-> remote" )
      :src-uri local-uri
      :src-queue (first queues);; FIXME are we sure there is just one? 
      :dest-uri (:remote-uri remote) 
      :dest-exchange (:exchange remote) 
      :prefetch-count 100
      :reconnect-delay 1
      :add-forward-headers false 
      :ack-mode "on-publish"}
     ; shovel FROM the remote rabbitmq
     {:resource :shovel
      :vhost vhost
      :name (str  "remote ->" vhost)
      :src-uri (:remote-uri remote)
      :src-queue (:queue remote) 
      :dest-uri local-uri   
      :dest-exchange ex 
      :prefetch-count 100
      :reconnect-delay 1
      :add-forward-headers false 
      :ack-mode "on-publish"}
     ; bindings within ppu-vhost froinvalidm remote users via this proxy user
     (for [[ccollection-id cov-ids] collections, 
           cov-id cov-ids
           :let [{:keys [from to tag]} (get covenants cov-id)]
           :when (contains? aliases from)
           :let [from-remote remote-user]]  
       [{:resource :exchange-binding
         :vhost ppu-vhost
         :from ex;(user-exchange-write-internal from-remote) 
         :to (feat/user-exchange-read to)
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
       {:resource :exchange-binding 
        :vhost ppu-vhost
        :from (feat/user-exchange-write-internal from) 
        :to (feat/user-exchange-read to)
        :arguments {:routing_key (format "*.%s.%s" tag ccollection-id) 
                    :arguments {}}})]))