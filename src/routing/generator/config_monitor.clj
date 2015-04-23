(ns routing.generator.config-monitor
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

(defn run-configuration-watcher! [opts atom]
  (let [conn (rmq/connect opts)
        ch (lch/open conn)
        queue (lq/declare-server-named ch)
        rks ["queue" "binding" "exchange" "user" "vhost" "permission"]
        actions ["created" "deleted"]]
    (lq/bind ch queue "amq.rabbitmq.event" {:routing-key "#"})
    (lc/subscribe ch queue 
                  (fn [_ hdrs body]
                    ))
    conn))