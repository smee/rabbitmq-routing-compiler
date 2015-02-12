(ns routing.routing-rest
  (:require [clojure.string :refer [join]]
            [clojure.data.json :refer [write-str]]
            [clojure.tools.logging :as log :refer [info error]]
            [routing.generator.routingkey :as gen]
            [routing.generator :as generator] 
            [routing.contracts :as con]
            [routing.generator.io :as io]
            [routing.generator.viz :as viz]
            [ring.adapter.jetty :as jetty] 
            [ring.middleware 
             [keyword-params :refer [wrap-keyword-params]]
             [json :refer [wrap-json-params]]
             [params :refer [wrap-params]]]            
            [liberator.core :refer [defresource resource]]
            liberator.dev
            [cheshire.core :as json]
            [compojure 
             [core :refer [routes ANY GET]]
             [handler :refer [api]]]
            [clojure.set :as cs]
            [org.clojars.smee.map :refer [map-values]]
            [schema
             [core :as s]
             [coerce :as sc]
             [utils :as su]] 
            )
  (:gen-class))
(defonce management-api (atom {:management-url (format "http://%s:%s" 
                                                       (get (System/getenv) "RABBITMQ_PORT_15672_TCP_ADDR" "127.0.0.1")
                                                       (get (System/getenv) "RABBITMQ_PORT_15672_TCP_PORT" "15672"))
                               :management-user (get (System/getenv) "user" "guest")
                               :shovel-user (get (System/getenv) "shovel_user" "shovel")
                               :shovel-password (get (System/getenv) "shovel_password" "shovel")
                               :shovel-password-hash (get (System/getenv) "shovel_password_hash" "1iKHKKKMGQS2fF6CRN/S7y5wg9M=")
                               :management-password (get (System/getenv) "password" "guest")
                               :ppu-vhost (get (System/getenv) "ppu-vhost" "VH_ppu")})) 

; map collections, tags etc. to rabbitmq resources on every change
(when (not *compile-files*) 
  (remove-watch con/contracts :routing-compiler)
  (add-watch con/contracts :routing-compiler
             (fn [_ _ old new] 
               (when true;(not= old new) 
                 (generator/update-routing! 
                   #_new @con/contracts 
                   @management-api)))))
;;;; helpers
(defn build-entry-url [{:keys [scheme server-name server-port uri]} nested? & ids] 
  (let [path (join "/" (map str ids))
        uri (if (.endsWith uri "/") (subs uri 0 (dec (count uri))) uri)
        path (if nested? (str uri "/" path) path)] 
    (format "%s://%s:%s%s"
            (name scheme)
            server-name
            server-port
            path)))

(defn- post? [r] (= :post (get-in r [:request :request-method])))

(defresource rendering-resource [vhost {:keys [with-ae? 
                                               with-shovels?
                                               with-federation?
                                               start-vhost 
                                               start-exchange 
                                               routing-key]}]
  :allowed-methods [:get]
  :available-media-types ["image/png"]
  :handle-ok (fn [ctx] 
               (let [creds @management-api
                     vhosts (if (string? vhost) [vhost] vhost)
                     declarations (if vhosts 
                                    (mapcat #(concat (routing.generator.io/fetch-routing % creds :incl-federation? with-federation?)
                                                     (routing.generator.io/fetch-shovels % creds)) vhosts)
                                    (mapcat #(map (fn [decl] (assoc decl :host (select-keys (meta %) [:name :aliases]))) 
                                                  (generator/create-all % 
                                                                                        (-> creds
                                                                                          (merge (meta %))
                                                                                          (select-keys (keys io/+Credentials+)))))
                                            [@con/contracts]))]
                 (-> declarations
                   (viz/routing->graph :with-ae? with-ae? :with-shovels? with-shovels?
                                       :start-vhost start-vhost :start-exchange start-exchange 
                                       :routing-key routing-key)
;                   (->> (#(do (println %) %)))
                   viz/dot->png
                   (java.io.ByteArrayInputStream.)))))


(defn underscore->minus 
  "walk data and replace _ by - in all keywords."
  [data]
  (clojure.walk/postwalk #(if (keyword? %) 
                            (-> % name (clojure.string/replace "_" "-") keyword)
                            %) data))

(defn minus->underscore 
  "walk data and replace - by _ in all keywords."
  [data]
  (clojure.walk/postwalk #(if (keyword? %) 
                            (-> % name (clojure.string/replace "-" "_") keyword)
                            %) data))

(defn json-coercion-matcher
    "A matcher that coerces keywords and keyword enums from strings, and longs and doubles
     from numbers on the JVM (without losing precision)"
    [schema] 
    (or (sc/json-coercion-matcher schema)
        (cond
          (= s/Str schema) (sc/safe name)
          (set? schema) set)))

(def contracts-json-to-clj (sc/coercer con/+Contracts+ json-coercion-matcher))

(defresource management-api-resource
  :available-media-types ["application/json"]
  :allowed-methods [:post :get]
  :malformed? (fn [r] ;(clojure.pprint/pprint r)
               (when (post? r)
                 (let [credentials (underscore->minus (get-in r [:request :params]))] 
                   [(not (= #{:management-url :management-user :management-password :ppu-vhost}
                            (set (keys credentials)))) 
                    {::data credentials}])))
  :post! #(reset! management-api (::data %))
  :handle-ok (fn [_] (minus->underscore @management-api)))


(defresource everything
  :available-media-types ["application/json"]
  :allowed-methods [:post :get]
  :malformed? (fn [r]
                (when (post? r)
                  (let [contracts (underscore->minus (get-in r [:request :params]))
;                        _ (def contracts contracts)
                        data (contracts-json-to-clj contracts)]
                    [(su/error? data) {::data data}]))) 
  :handle-malformed #(do (info "was called") 
                       (pr-str (:error (::data %)))) 
  :post! #(reset! con/contracts (::data %))
  :handle-ok (fn [_] (minus->underscore @con/contracts))) 

#_(def dbg-handler (fn [handler]
                      (fn [req]
                        (println (:params req))
                        (handler req))))
(def handler
  (-> 
    (routes
      (GET "/" [] (resource :allowed-methods [:get]
                            :available-media-types ["application/json"] 
                            :handle-ok #(vector
                                          (build-entry-url (:request %) true "all")
                                          (build-entry-url (:request %) true "management")
                                          (build-entry-url (:request %) true "routing.png"))))
      (ANY "/management" [] management-api-resource)
      (ANY "/all" [] everything) 
      (GET "/routing.png" [ae fed shovels vhost start-vhost start-exchange routing-key] 
           (rendering-resource vhost 
                               {:with-ae? (Boolean/parseBoolean ae) 
                               :with-shovels? (Boolean/parseBoolean shovels)
                               :with-federation? (Boolean/parseBoolean fed)
                               :start-vhost start-vhost
                               :start-exchange start-exchange
                               :routing-key routing-key})))
    wrap-keyword-params
    (wrap-json-params {:on-error (fn [handler req ex] (error ex) (.printStackTrace ex) {:status 400 :body "Malformed JSON."})})
    wrap-params
    (liberator.dev/wrap-trace :ui #_:header)))

(defn start [options]
  (jetty/run-jetty #'handler (assoc options :join? false)))

(defn -main
  ([port]
    (schema.core/set-fn-validation! true)
    (start {:port (Integer/parseInt port)}))
  ([]
    (-main "5000")))