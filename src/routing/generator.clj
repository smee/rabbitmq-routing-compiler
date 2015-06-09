(ns ^{:doc "Configuration logic for RabbitMQ. This is where the magic happens."}
routing.generator
  (:require [routing.schemas :refer [+Credentials+ +Contracts+]]
            [routing.generator
             [common :refer [as-flat-set]]
             [features :as gen]
             [io :as io :refer [with-credentials]]
             [rabbit-password :refer [rabbit-password-hash]]]
            [routing.generator.features
             [delegation :as de]
             [local-users :as lu]] 
            [clojure.set :refer [difference]]
            [clojure.tools.logging :as log :refer [info infof debugf debug]]
            [schema.core :as s])
  (:import java.net.URLEncoder
           clojure.lang.IFn))

;; FIXME just sorting is not enough:
;; - deleting a user deletes its permissions
;; - deleting a queue if there is a shovel accessing it does not work (need to delete shovel first because shovel will recreate the queue (with default queue arguments!))
;; - shovels don't like user_id headers: might work within one MOM, breaks when shoveling between MOMs
;; 

(defn- priority-of [m]
  (let [order {:user 0
               :vhost 1
               :permission 2
               :exchange 3
               :queue 4
               :exchange-binding 5
               :queue-binding 6
               :policy 7
               :federation-upstream 8
               :federation-upstream-set 9
               :federation-policy 10
               :shovel 11
               :tracing 12}] 
    (order (:resource m))))

(defn declaration-comparator 
  "Declare half order of declarations. Declarations may have dependencies, i.e.
   structures that have to be present for a declaration to succeed. The dependencies are:
   ex < alternate ex
   ex < vhost
   q < vhost
   binding < ex
   binding < q
   permission < user
   permission < vhost
   ex|q|binding|federation < permission config
   federation-upstream|-upstream-set|policy < vhost|ex
  "
  [o1 o2]
  {:pre [(map? o1) (map? o2)]}
  (let [prio1 (priority-of o1)
        prio2 (priority-of o2)] 
    (cond 
      (< prio1 prio2) -1
      (> prio1 prio2) 1
      :else 0))) 

(s/defn fetch-all 
  "Fetch all existing structures from a RabbitMQ instance in parallel."
  [creds :- +Credentials+]
  (let [vhosts (io/fetch-vhosts creds)] 
    (as-flat-set
      (pvalues
        vhosts
        (io/fetch-users creds)
        (io/fetch-permissions creds)
        (io/fetch-admin-permissions creds))
      (pmap (fn [vh]
              (pvalues (io/fetch-routing vh creds) 
                       (io/fetch-federations vh creds)
                       (io/fetch-shovels vh creds)
                       (io/fetch-policies vh creds) 
                       (io/fetch-federations vh creds)
                       (io/fetch-tracing-settings vh creds)))
            (map :name vhosts)))))

(s/defn create-all-separate-vhosts
  "Create all declarations for elements in a rabbitmq instance given the contracts data structure 
and the credentials."
  [contracts :- +Contracts+
   credentials :- +Credentials+] 
  (apply as-flat-set
         ((juxt gen/construct-users
                gen/construct-user-vhosts
                gen/construct-permissions
                gen/generate-private-resources-for-users
                gen/construct-private-queue-bindings
                lu/construct-localusers
                lu/construct-localuser-covenants                
                gen/construct-admin-declarations 
                gen/construct-admin-permissions-for-vhosts
                gen/construct-routing-key-only
                de/construct-delegation-routing
                de/construct-transparent-delegation-routing
                gen/construct-tracing
                gen/construct-unroutable 
                gen/construct-internal-shovel-user
                gen/construct-internal-shovels
                gen/construct-high-availability-for-queues) 
           contracts credentials (partial str "VH_"))))

(s/defn create-all-single-vhost
  "Create all declarations for elements in a rabbitmq instance given the contracts data structure 
and the credentials."
  [contracts :- +Contracts+
   credentials :- +Credentials+]
  (as-flat-set
    ((juxt gen/construct-users
           gen/construct-permissions
           gen/generate-private-resources-for-users
           gen/construct-private-queue-bindings
           lu/construct-localusers
           lu/construct-localuser-covenants
           gen/construct-admin-declarations 
           gen/construct-routing-key-only
           de/construct-delegation-routing
           de/construct-transparent-delegation-routing
           gen/construct-tracing
           gen/construct-unroutable
           gen/construct-internal-shovel-user
           gen/construct-high-availability-for-queues)
      contracts credentials (constantly (:ppu-vhost credentials)))))

(defn get-generator-fn [key] 
  (get {:single create-all-single-vhost
        :separate create-all-separate-vhosts} (or key :single)))

(s/defn update-routing! 
  "Synchronize declarations derived from `contracts` and `credentials` with the configuration
currently present within a rabbitmq instance." 
  [contracts :- +Contracts+ 
   credentials :- +Credentials+
   routing-constructor-fn :- IFn]
  (with-credentials credentials 
    (let [decls (sort-by identity declaration-comparator (routing-constructor-fn contracts credentials))
          existing (sort-by identity declaration-comparator (fetch-all credentials)) 
          decl-set (set decls)] 
      ; delete declarations not needed in reverse sorted order
      (doseq [decl (reverse existing) 
              :when (not (contains? decl-set decl))
              :let [vh (:vhost decl)]]
        (debug "deleting" decl)
        (io/remove-declaration! vh decl))
      ;add new declarations
      ;fetch all existing data AGAIN, because there may have happened implicit deletes
      ;for example: deleting a user deletes his permissions, too.
      (let [existing-set (set (fetch-all credentials))] 
        (doseq [decl decls 
                :when (not (contains? existing-set decl))
                :let [vh (:vhost decl)]]
          (debug "adding" decl) 
          (io/apply-declaration! vh decl))))))
 

(defn set-tracing! 
  "Enable tracing for an individual vhost. Refer to http://www.rabbitmq.com/firehose.html"
  [vhost enabled? credentials]
  (with-credentials credentials
    (if enabled? 
      (io/apply-declaration! vhost {:resource :tracing :action :declare})
      (io/remove-declaration! vhost {:resource :tracing :action :declare}))))

(defn set-big-bang-migration! [enabled? contracts credentials]
  (with-credentials credentials
    (let [credentials (assoc-in credentials [:management :url] "http://localhost:15673")
          features (gen/construct-big-bang-migrations contracts credentials (fn [_]))
          features (sort-by identity declaration-comparator features)
          fun (if enabled? io/apply-declaration! io/remove-declaration!)]
      (doseq [f features] (println f)
        (println (fun (:ppu-vhost credentials) f))))))

(defn set-queues-based-migration! [enabled? contracts credentials-old credentials-new]
  (let [fun (if enabled? io/apply-declaration! io/remove-declaration!)
        vh (:ppu-vhost credentials-old)]
    (with-credentials credentials-old
      (doseq [f [{:resource :federation-upstream 
                  :vhost vh
                  :name "new-cluster-upstream" 
                  :uri (str "amqp://guest:guest@localhost:5673/" vh)}
                 {:resource :federation-policy
                  :vhost vh
                  :federation-upstream-set "all"
                  :name "new-cluster-migration-policy"
                  :apply-to "queues"
                  :pattern (str "^.*-q-.*$")}]]
        (println (fun vh f) f)))
    (with-credentials credentials-new
      (doseq [f [{:resource :federation-upstream 
                  :vhost vh
                  :name "old-cluster-upstream" 
                  :uri (str "amqp://guest:guest@localhost:5672/" vh)}
                 {:resource :federation-policy
                  :vhost vh
                  :federation-upstream-set "all"
                  :name "old-cluster-migration-policy"
                  :apply-to "queues"
                  :pattern (str "^.*-q-.*$")}]]
        (println (fun vh f) f)))))

(comment
  (set-big-bang-migration! 
    false 
    @routing.contracts/contracts
    @routing.routing-rest/management-api)
  
  (set-queues-based-migration!
    true 
    @routing.contracts/contracts
    @routing.routing-rest/management-api
    (assoc-in @routing.routing-rest/management-api [:management :url] "http://localhost:15673"))
  
  (time (update-routing! 
          ;routing.contracts/empty-contracts
          @routing.contracts/contracts
          @routing.rest.model/management-api
          ;create-all-separate-vhosts
          create-all-single-vhost
          ))
  
  (time (update-routing! 
          ;routing.contracts/empty-contracts
          ;@routing.contracts/contracts
          routing.contracts/demo-transparent-delegation
          @routing.rest.model/management-api
;          (assoc @routing.routing-rest/management-api :node-urls ["http://localhost:15673"])
          ;create-all-separate-vhosts
          create-all-single-vhost
          ))

  
  ; create all remote configurations for the demonstrator
  (doseq [config [@routing.contracts/contracts]
          :let [settings (merge @routing.routing-rest/management-api (meta config))
                config-name (or (:name settings) (-> settings :management :url))]]
    (info "configuring" config-name)
    (update-routing! config settings routing-constructor-fn))
  
  (set-tracing! "VH_ppu" true @routing.routing-rest/management-api)
  (set-tracing! "VH_ppu" false @routing.routing-rest/management-api)
  
  (let [groups (->> (create-all-single-vhost @routing.contracts/contracts @routing.routing-rest/management-api)
                 (sort-by identity declaration-comparator)
                 (partition-by :resource))]
    (doseq [decls groups]
      (clojure.pprint/print-table decls)))
  )

;; TODO store snapshots for recovery/rollback scenarios!
;; TODO while changing routes messages may get lost (delete bindings before adding new)
