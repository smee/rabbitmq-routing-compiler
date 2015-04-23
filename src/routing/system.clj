(ns routing.system
  (:require [com.stuartsierra.component :as component]
            [ring.adapter.jetty :as jetty]
            [langohr 
             [core      :as rmq]
             [channel   :as lch]]))

(defrecord JettyServer [_]
  component/Lifecycle
  (start [component]
    (if (:server component)
      component
      (let [options (-> component (dissoc :app) (assoc :join? false))
            server  (jetty/run-jetty (:handler component) options)]
        (assoc component :server server))))
  (stop [component]
    (if-let [^org.eclipse.jetty.server.Server server (:server component)]
      (do (.stop server)
          (.join server)
          (dissoc component :server))
      component)))


(defrecord RabbitMQConnection [user password host amqp-port]
  component/Lifecycle
  (start [component]
    (if (:connection component) 
      component
      (let [conn (rmq/connect {:username user :password password :host host :port amqp-port})
            ch (lch/open conn) ]
        (assoc component :connection conn :channel ch))))
  (stop [component]
    (when-let [c (:channel component)] (rmq/close c))
    (when-let [c (:connection component)] (rmq/close c))
    (dissoc component :channel :connection)))

