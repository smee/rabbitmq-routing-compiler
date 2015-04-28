(ns routing.rest.model
  (:require [routing.contracts :as con]
            [routing.generator :as generator]))

(defonce management-api 
  (atom {:cluster-name "local cluster"
         :node-urls [(format "http://%s:%s" 
                             (get (System/getenv) "RABBITMQ_PORT_15672_TCP_ADDR" "127.0.0.1")
                             (get (System/getenv) "RABBITMQ_PORT_15672_TCP_PORT" "15672"))]
         :management {:user (get (System/getenv) "user" "guest")
                      :password (get (System/getenv) "password" "guest")} 
         :shovel {:user (get (System/getenv) "shovel_user" "shovel")
                  :password (get (System/getenv) "shovel_password" "shovel")
                  :password-hash (get (System/getenv) "shovel_password_hash" "1iKHKKKMGQS2fF6CRN/S7y5wg9M=")} 
         :ppu-vhost (get (System/getenv) "ppu-vhost" "VH_ppu")})) 

; map collections, tags etc. to rabbitmq resources on every change
(when (not *compile-files*) 
  (let [watch-fn (fn [_ _ old new] 
                   (generator/update-routing! 
                     @con/contracts
                     @management-api
                     (generator/get-generator-fn :single)))] 
    (remove-watch con/contracts :routing-compiler)
    (add-watch con/contracts :routing-compiler watch-fn)
    (remove-watch management-api :routing-compiler)
    (add-watch management-api :routing-compiler watch-fn)))