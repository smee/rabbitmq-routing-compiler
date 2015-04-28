(ns routing.rest.server
  (:require [ring.adapter.jetty :as jetty]
            [compojure.api.sweet :refer :all]
            [routing.schemas :as s]
            [routing.generator :as generator]
            [routing.generator.io :as io]
            [routing.generator.viz :as viz]
            [routing.contracts :as con]
            [routing.rest.model :as model]
            [ring.util.http-response :as resp]
            [schema.coerce :as sc])
  (:gen-class))
;; workaround for bug in compojure-api: can't handle schemas with arbitrary keys (which we need for our current contracts data structure)
(defn json-coercion-matcher
  "A matcher that coerces keywords and keyword enums from strings, and longs and doubles
   from numbers on the JVM (without losing precision)"
  [schema]
  (or (sc/json-coercion-matcher schema)
      (cond
        (= String schema) (sc/safe name)
        (set? schema) set)))

(def contracts-json-to-clj (sc/coercer s/+Contracts-REST+ json-coercion-matcher))


(defapi handler
        (swagger-ui)
        (swagger-docs
          {:info {:version "1.0.0"
                  :title   "Routingcompiler"}
           :tags [{:name "credentials" :description "Connection to a RabbitMQ cluster"}
                  {:name "contracts" :description "Technical contracts for routing"}
                  {:name "viz" :description "Visualizations of routing structures"}
                  {:name "demos" :description "Demonstration contract configuration for different scenarios"}]})
        (context* "/visualization" []
                  :tags ["viz"]
                  (GET* "/routing.png" []
                        :query-params [{simulation :- (describe Boolean "simulate or fetch from RMQ?") true}
                                       {start-vhost :- (describe String "send a message in this vhost") nil}
                                       {start-exchange :- (describe String "send a message from this exchange") nil}
                                       {routing-key :- (describe String "send a message using this rk") nil}
                                       {strategy :- (describe (schema.core/enum :single :separate) "configuration strategy") :single}]
                        (-> (let [generator-fn (generator/get-generator-fn strategy)
                                  creds @model/management-api
                                  declarations (if simulation
                                                 (generator-fn @con/contracts creds)
                                                 (let [vhosts (map :name (io/fetch-vhosts creds))]
                                                   (mapcat #(concat (io/fetch-routing % creds)
                                                                    (io/fetch-shovels % creds)) vhosts)))]
                              (-> declarations
                                  (viz/routing->graph {:start-vhost start-vhost
                                                       :start-exchange start-exchange
                                                       :routing-key routing-key})
                                  ;                   (->> (#(do (println %) %)))
                                  viz/dot->png
                                  (java.io.ByteArrayInputStream.)))
                          (resp/ok)
                          (resp/content-type "image/png"))))
        (context* "/api" []
                  :tags ["contracts"]
                  (GET* "/contracts" []
                         :return s/+Contracts-REST+
                         :summary "Currently valid contracts/user definitions"
                         (resp/ok (s/internal->external-contracts @routing.contracts/contracts)))
                  (POST* "/contracts" {body :body-params}
                        :return s/+Contracts-REST+
                        :body [cs s/+Contracts-REST+]
                        :summary "set new contracts/user definitions, update RabbitMQ"
                         (let [contracts (contracts-json-to-clj body)]
                           (if (schema.utils/error? contracts)
                             (resp/bad-request (schema.utils/error contracts))
                             (-> contracts 
                               s/external->internal-contracts 
                               routing.contracts/replace-contracts! 
                               s/internal->external-contracts 
                               resp/ok)))))
        (context* "/demo" []
                  :tags ["demos"]
                  (GET* "/contracts-demo" []
                         :return s/+Contracts-REST+
                         :summary "some demo contracts in the application domain of power plant maintenance"
                         (resp/ok (s/internal->external-contracts routing.contracts/demo-contracts)))
                  (GET* "/contracts-delegation" []
                        :return s/+Contracts-REST+
                        :summary "Demo contract data structure for non-transparent delegation of a covenant"
                        (resp/ok (s/internal->external-contracts routing.contracts/demo-direct-delegation)))
                  (GET* "/contracts-transparent-delegation" []
                        :return s/+Contracts-REST+
                        :summary "Demo contract data structure for transparent delegation of a covenant"
                        (resp/ok (s/internal->external-contracts routing.contracts/demo-transparent-delegation)))
                  (GET* "/contracts-local-users" []
                        :return s/+Contracts-REST+
                        :summary "delegate covenants to a local user (subordinate of a platform user)"
                        (resp/ok (s/internal->external-contracts routing.contracts/demo-local-users))))
        (context* "/api" []
                  :tags ["credentials"]
                  (GET* "/management" []
                        :return s/+Credentials+
                        :summary "Credentials for a RabbitMQ cluster"
                        (resp/ok @model/management-api))
                  (POST* "/management" []
                         :return s/+Credentials+
                         :body [creds s/+Credentials+]
                         :summary "Change credentials for a RabbitMQ cluster"
                        (resp/ok (reset! model/management-api creds)))))

(defn start [options]
  (jetty/run-jetty #'handler (assoc options :join? false)))

(defn -main
  ([port]
   (schema.core/set-fn-validation! true)
   (start {:port (Integer/parseInt port)}))
  ([]
   (-main "5000")))