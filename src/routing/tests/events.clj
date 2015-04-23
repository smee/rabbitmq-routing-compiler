(ns routing.tests.events
  (:require [clojure.tools.logging :as log :refer [info infof]]
            [routing.tests.logging-client :refer [logging-consumer-wo-payload]]
            [common.rabbit :refer [with-connection]]
            [langohr 
             [core      :as rmq]
             [channel   :as lch]
             [exchange  :as le] 
             [queue     :as lq]
             [consumers :as lc]
             [basic     :as lb]
             [shutdown :as ls]]))

(defn run-event-listener! [opts]
  (let [conn (rmq/connect opts)
        ch (lch/open conn)
        queue (lq/declare-server-named ch)]
    (lq/bind ch queue "amq.rabbitmq.event" {:routing-key "#"})
    (lc/subscribe ch queue (partial logging-consumer-wo-payload "event listener") {:auto-ack true})
    conn))

(defn consume-all-messages! [opts]
  (let [conn (rmq/connect opts)
        ch (lch/open conn)
        q (:queue opts)]
    (lc/subscribe ch q (partial logging-consumer-wo-payload q) {:auto-ack false})
    conn))

(defn send-a-message [{:keys [exchange routing-key] :as opts}]
  (with-connection [conn (rmq/connect opts)
                    ch (lch/open conn)]
    (lb/publish ch exchange routing-key "payload")))

(comment
  (def listener-conn (run-event-listener! {:vhost "/" :port 5672}))
  (def consumer-conn (consume-all-messages! {:vhost "VH_ppu" :port 5672
                          :username "archiver" :password ""
                          :queue "archiver-q-0"}))
  (send-a-message {:vhost "VH_ppu" 
                   :port 5672
                   :username "scada" 
                   :password "scada"
                   :exchange "scada-ex-write"
                   :routing-key "scada.data.ALL"})
  )