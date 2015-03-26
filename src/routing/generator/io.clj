(ns routing.generator.io
  (:require [langohr.http :as lh]
            [org.clojars.smee.map :refer [map-values]]
            [routing.schemas :as schemas]
            [routing.generator
             [common :refer [as-flat-set]]
             [features :as gen]]               
            [clojure.set :refer [difference]]
            [clojure.tools.logging :as log :refer [info infof warnf debug]])
  (:import java.net.URLEncoder))


(defn ^:private http-method [langohr-delegation-fn]
  (fn 
    ([url] 
      (langohr-delegation-fn (lh/url-with-path url) {}))
    ([url body] 
      (langohr-delegation-fn (lh/url-with-path url) {:body body}))))


(def ^:private GET (http-method (fn [url body]
                                  (let [{:keys [body status] :as response} (#'lh/get url body)]
                                    (if (<= 200 status 299)
                                      (#'lh/safe-json-decode response)
                                      (do 
                                        (warnf "Error accessing url %s: %s" url body)
                                        []))))))
(def ^:private PUT (http-method #'lh/put))
(def ^:private POST (http-method #'lh/post))
(def ^:private DELETE (http-method #'lh/delete))

(defn- url [url-template & params] 
  (apply format url-template (map #(URLEncoder/encode %) params)))


(defmacro with-credentials [creds & body]
  `(let [creds# ~creds]
     (binding [lh/*endpoint* (-> creds# :management :url ) 
               lh/*username* (-> creds# :management :user) 
               lh/*password* (-> creds# :management :password)]
       ~@body))) 

(defn fetch-routing 
  "Load exhanges, queues and bindings via RabbitMQ's http api and return the canonical representation
used by `construct-routing`."
  [vhost creds & {:keys [incl-federation?]}]
  (debug "fetching RabbitMQ configuration for vhost " vhost) 
  (with-credentials creds
    (let [skip? (fn [n] 
                  (or (empty? n)
                      (and (not incl-federation?) (re-matches #"federation.*" n))
                      (re-matches #"amq\..*" n)))] 
      (as-flat-set
        ; fetch exchanges
        (for [{n :name :as decl} (lh/list-exchanges vhost)
              :when (and (not (skip? n)))]
          {:action :declare 
           :resource :exchange 
           :vhost vhost
           :arguments (select-keys decl [:name :type :durable :auto_delete :internal :arguments])})
        ; fetch queues
        (for [{n :name :as decl} (lh/list-queues vhost)
              :when (not (skip? n))]
          {:action :declare 
           :resource :queue
           :vhost vhost
           :arguments (select-keys decl [:name :type :auto_delete :durable :arguments])})
        ; fetch bindings
        (for [{:keys [source destination destination_type routing_key arguments]} (lh/list-bindings vhost)
              :when (not (or (empty? source)
                             (empty? destination)
                             (and (skip? source) (skip? destination))))]
          {:action :bind 
           :resource (keyword destination_type)
           :vhost vhost
           :from source
           :to destination       
           :arguments {:routing_key routing_key :arguments arguments}})))))

(defn fetch-federations 
  [vhost creds]
  (with-credentials creds 
    (as-flat-set
      (for [{n :name vh :vhost p :pattern {us :federation-upstream-set} :definition} 
            (GET (url "/api/policies/%s" vhost)) 
            :when (not (nil? us))]
        {:action :declare
         :resource :federation-policy
         :name n
         :vhost vh
         :pattern p
         :federation-upstream-set us})
      (for [{vh :vhost n :name {uri :uri ex :exchange} :value} 
            (GET (url "/api/parameters/federation-upstream/%s" vhost))]
        {:resource :federation-upstream
         :action :declare
         :vhost vh
         :name n
         :uri uri})
      (for [{vh :vhost n :name [{us :upstream ex :exchange}] :value} 
            (GET (url "/api/parameters/federation-upstream-set/%s" vhost))]
        {:resource :federation-upstream-set
         :action :declare
         :vhost vh
         :name n
         :upstream us
         :exchange ex}))))

(def ^:private shovel-keys [:src-uri :src-queue :dest-uri :dest-exchange :prefetch-count :reconnect-delay :add-forward-headers :ack-mode])

(defn fetch-shovels
  [vhost creds]
  (with-credentials creds
    (as-flat-set
      (for [{n :name v :value} (GET (url "/api/parameters/shovel/%s" vhost))]
        (assoc (select-keys v shovel-keys)
               :resource :shovel
               :action :declare
               :vhost vhost
               :name n)))))

(defn fetch-tracing-settings
  [vhost creds]
  (with-credentials creds
    (as-flat-set
      (when (:tracing (GET (url "/api/vhosts/%s" vhost)))
        [{:resource :tracing
          :action :declare
          :vhost vhost}]))))

(defn fetch-users 
  [creds]
  (with-credentials creds
    (as-flat-set 
      (for [{:keys [name password_hash tags]} (filter #(= "generated" (:tags %)) (lh/list-users))]
        {:action :declare
         :resource :user
         :name name
         :password_hash password_hash
         :tags tags}))))

(defn fetch-vhosts
  "Fetch all vhost starting with \"VH_\". "
  [creds]
  (with-credentials creds
    (as-flat-set
      (let [vhosts (filter gen/generated-vhost? (map :name (lh/list-vhosts)))            ] 
        (for [vh vhosts] 
          {:action :declare
           :resource :vhost
           :name vh})))))

(defn fetch-permissions
  "Fetch all permissions of all generated users."
  [creds]
  (with-credentials creds
    (as-flat-set
      (let [users (->> (lh/list-users)
                    (filter #(= "generated" (:tags %)))
                    (map :name)
                    set)
            permissions (filter (comp users :user) (lh/list-permissions))] 
        (for [{:keys [user vhost configure read write]} permissions]
          {:action :declare
           :resource :permission
           :user user 
           :vhost vhost
           :write write 
           :read read 
           :configure configure})))))

(defn fetch-admin-permissions
  "Fetch all permissions of the given administrator user within generated vhosts."
  [creds]
  (with-credentials creds
    (as-flat-set
      (let [admin-users #{(-> creds :shovel :shovel-user) (-> creds :management :user)}
            admin-permissions (filter #(and (gen/generated-vhost? (:vhost %)) 
                                            (admin-users (:user %))) 
                                      (lh/list-permissions))]
        (for [{:keys [vhost configure read write user]} admin-permissions]
          {:action :declare
           :resource :permission
           :user user 
           :vhost vhost 
           :write write 
           :read read 
           :configure configure})))))

;;;;
;;;; apply configurations to RabbitMQ
;;;;
(defmulti apply-declaration! "Apply a configuration to a RabbitMQ instance" (fn [vhost decl] [(:resource decl) (:action decl)]))

(defmethod apply-declaration! [:exchange :declare] [vhost {{n :name :as args} :arguments}] 
  (lh/declare-exchange vhost n args))

(defmethod apply-declaration! [:queue :declare] [vhost {{n :name :as args} :arguments}] 
  (lh/declare-queue vhost n args))

(defmethod apply-declaration! [:exchange :bind] [vhost {:keys [from to arguments]}]
  (POST (url "/api/bindings/%s/e/%s/e/%s" vhost from to) arguments))

(defmethod apply-declaration! [:queue :bind] [vhost {:keys [from to arguments]}]
  (lh/bind vhost from to arguments))

(defmethod apply-declaration! [:user :declare] [vhost {:keys [name password_hash tags] :as params}]
  (PUT (url "/api/users/%s" name) {:password_hash password_hash :tags tags}))

(defmethod apply-declaration! [:permission :declare] [vhost {:keys [user vhost configure write read] :as params}] 
  (lh/declare-permissions vhost user params)) 

(defmethod apply-declaration! [:vhost :declare] [_ {vhost :name}] 
  (lh/declare-vhost vhost))

(defmethod apply-declaration! [:federation-upstream :declare] [vhost {:keys [name uri]}]
  (let [uss-name (str name "-set")]
    (PUT (url "/api/parameters/federation-upstream/%s/%s" vhost name) 
         {:value {:uri uri :ack-mode "on-confirm" :trust-user-id true
                  :max-hops 1000}
          :name name
          :vhost vhost
          :component "federation-upstream"}))) 

(defmethod apply-declaration! [:federation-upstream-set :declare] [_ {:keys [name upstream vhost exchange]}]
  (PUT (url "/api/parameters/federation-upstream-set/%s/%s" vhost name) 
       {:value [{:upstream upstream
                 :exchange exchange}]
        :name name
        :vhost vhost
        :component "federation-upstream-set"}))

(defmethod apply-declaration! [:federation-policy :declare] [_ {:keys [vhost pattern federation-upstream-set name]}]
  (lh/declare-policy vhost name 
                     {:pattern pattern 
                      :definition {:federation-upstream-set federation-upstream-set} 
                      :priority 0}))

(defmethod apply-declaration! [:shovel :declare] [vhost {n :name :as params}]
  (let [value (select-keys params shovel-keys)] 
    (PUT (url "/api/parameters/shovel/%s/%s" vhost n) 
         {:value value})))

(defmethod apply-declaration! [:tracing :declare] [vhost _]
  (PUT (url "/api/vhosts/%s" vhost) {:tracing true})) 
;;;;
;;;; remove configurations from RabbitMQ
;;;;
(defmulti remove-declaration! "Delete a configuration item in RabbitMQ" (fn [vhost decl] [(:resource decl) (:action decl)]))

(defmethod remove-declaration! [:exchange :declare] [vhost {{n :name} :arguments}]
  (lh/delete-exchange vhost n))

(defmethod remove-declaration! [:queue :declare] [vhost {{n :name :as params} :arguments}] 
  (lh/delete-queue vhost n))

(defmethod remove-declaration! [:exchange :bind] [vhost {from :from, to :to {:keys [routing_key arguments]} :arguments}]
  (let [potentials (filter #(and (= (:routing_key %) routing_key)
                                 (= (:destination %) to)) 
                           (GET (url "/api/bindings/%s/e/%s/e/%s" vhost from to)))]
    (when (= 1 (count potentials))
      (DELETE (url "/api/bindings/%s/e/%s/e/%s/%s"
                   vhost
                   from
                   to
                   (:properties_key (first potentials)))))))

(defmethod remove-declaration! [:queue :bind] [vhost {from :from, to :to {:keys [routing_key arguments]} :arguments}]
(let [potentials (filter #(and (= (:routing_key %) routing_key)
                                 (= (:destination %) to)) 
                         (GET (url "/api/bindings/%s/e/%s/q/%s" vhost from to)))]
  (when (= 1 (count potentials))
    (DELETE (url "/api/bindings/%s/e/%s/q/%s/%s"
                 vhost
                 from
                 to
                 (:properties_key (first potentials)))))))

(defmethod remove-declaration! [:user :declare] [vhost params]
  (lh/delete-user (:name params)))

(defmethod remove-declaration! [:vhost :declare] [vhost {vhost :name}] 
  (lh/delete-vhost vhost))

(defmethod remove-declaration! [:permission :declare] [vhost {:keys [user vhost] :as params}]
  (DELETE (url "/api/permissions/%s/%s" vhost user)))

(defmethod remove-declaration! [:federation-upstream :declare] [vhost {:keys [name]}]
  (DELETE (url "/api/parameters/federation-upstream/%s/%s" vhost name))) 

(defmethod remove-declaration! [:federation-upstream-set :declare] [_ {:keys [name vhost]}] 
  (DELETE (url "/api/parameters/federation-upstream-set/%s/%s" vhost name)))

(defmethod remove-declaration! [:federation-policy :declare] [_ {:keys [vhost name]}]
  (DELETE (url "/api/policies/%s/%s" vhost name)))

(defmethod remove-declaration! [:shovel :declare] [vhost {n :name}] 
  (DELETE (url "/api/parameters/shovel/%s/%s" vhost n)))

(defmethod remove-declaration! [:tracing :declare] [vhost _]
  (PUT (url "/api/vhosts/%s" vhost) {:tracing false}))
