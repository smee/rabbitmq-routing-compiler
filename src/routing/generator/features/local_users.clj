(ns routing.generator.features.local-users
  (:require [routing.generator.features :as feat :refer [deffeature]]
            [clojure.string :refer [join]]))

;;;;;;;;;;;;;;; Local users ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- find-pf-user-for-local [users local-user-name]
  (some (fn [[_ pf-user]]
          (when (get-in pf-user [:localusers local-user-name])
            pf-user)) users))

(deffeature construct-localuser-covenants 
  "localuser may have convenant with another local user. In this case (assuming they are both
localusers of the same platform user), we can directly bind from the sender's exchange
to the allocated queue."
  [{:keys [users collections covenants]} _ vhost-of]  
  (let [locals-per-pf-user (map (comp set keys :localusers) (vals users))] 
    (for [[cov-id {:keys [from to tag]}] covenants 
          :when (some #(and (% from) (% to)) locals-per-pf-user)
          :let [pf-user (find-pf-user-for-local users to)
                local-user-exchange (get-in pf-user [:localusers from :exchange])
                queues (get-in pf-user [:allocations cov-id])]]
      (for [q queues]
        {:resource :queue-binding 
         :vhost (vhost-of (:name pf-user)) 
         :from  local-user-exchange
         :to q 
         :arguments {:routing_key (format "%s.%s.*" from tag) :arguments {}}}))))

(deffeature construct-localusers 
  "Construct local users associated with a toplevel platform user. A platform user
may delegate covenants to its local users."
  [{:keys [users collections covenants]} _ vhost-of] 
  (for [[pf-user {lu :localusers pf-user-exchange :exchange}] users,
        {:keys [name password exchange queues delegation]} (vals lu),
        :let [vh (vhost-of pf-user)]]
    
    [; create local user
     {:resource :user 
      :name name 
      :password_hash password 
      :tags "generated"}
     ; localuser has his own writable exchange
     {:resource :exchange
      :vhost vh
      :arguments {:name exchange 
                  :type "topic" 
                  :internal false 
                  :durable true 
                  :auto_delete false 
                  :arguments {:alternate-exchange feat/invalid_routing_key}}}
     ; localuser may write to his exchange, read specific queues
     {:resource :permission 
      :vhost vh
      :user name
      :configure "^$" 
      :write exchange
      :read (join "|" queues)}
     ; localuser has bindings from his private exchange to its platform user's exchange
     ; for every covenant in every covenant-collection he is allowed to use
     (for [cov-id delegation, 
           [cov-coll-name cov-coll-ids] collections
           :when (contains? cov-coll-ids cov-id)
           :let [tag (get-in covenants [cov-id :tag])]]
       {:resource :exchange-binding 
        :vhost vh
        :from exchange 
        :to pf-user-exchange 
        :arguments {:routing_key (format "%s.%s.%s" pf-user tag cov-coll-name) :arguments {}}})
     (for [cov-id delegation, 
           :let [tag (get-in covenants [cov-id :tag])]]
       {:resource :exchange-binding 
        :vhost vh
        :from exchange 
        :to pf-user-exchange 
        :arguments {:routing_key (format "%s.%s" pf-user tag) :arguments {}}})]))

