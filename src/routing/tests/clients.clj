(ns routing.tests.clients
  (:require [clojure.tools.logging :as log :refer [info infof]]
            [common.rabbit :refer [with-connection]]
            [langohr 
             [core      :as rmq]
             [channel   :as lch]
             [exchange  :as le] 
             [queue     :as lq]
             [consumers :as lc]
             [basic     :as lb]
             [shutdown :as ls]]))

(defn produce-and-consume [{:keys [exchange routing-key queue username] :as opts} messages]
  (let [ids (atom #{})] 
    (with-connection [conn (rmq/connect opts)
                      ch (lch/open conn)]
      (lb/qos ch 1)
      (lc/subscribe ch queue (fn on-confirm[ch {tag :delivery-tag} body]
                               (when (rmq/open? ch)
                                 (lb/ack ch tag))
                               (infof "%10s: recv %s (%s)" username (String. body) tag)
                               (Thread/sleep 500)) {:auto-ack false})
      (doseq [msg messages]
        (Thread/sleep 100)
        (infof "%10s: send %s" username (str msg))
        (lb/publish ch exchange routing-key (str msg)))
      (Thread/sleep 1000))))



(comment
  
  (future (produce-and-consume {:exchange "scada-ex-write"
                                :queue "scada-q-0"
                                :vhost "VH_ppu"
                                :host "localhost"
                                :username "scada"
                                :password "scada"
                                :port 5672
                                :routing-key "scada.data.ALL"
                                }
                               (range 20)))
  (future (produce-and-consume {:exchange "analysis-ex-write"
                                :queue "analysis-q-0"
                                :vhost "VH_ppu"
                                :host "localhost"
                                :username "analysis"
                                :password "analysis"
                                :port 5673
                                :routing-key "analysis.storedata.ALL"
                                }
                               (range 20))))

(defn produce-and-consume-and-switch [{:keys [exchange routing-key queue username] :as opts} 
                                      {:keys [messages switch-every
                                              ports]}]
  (doseq [[port messages] (map vector (cycle ports) (partition-all switch-every messages))]
    (infof "%10s: switching to instance at port %d" username port)
    (produce-and-consume (assoc opts :port port)
                         messages)))

(comment
  (future (produce-and-consume-and-switch 
            {:exchange "scada-ex-write"
             :queue "scada-q-0"
             :vhost "VH_ppu"
             :host "localhost"
             :username "scada"
             :password "scada"
             :port 5672
             :routing-key "scada.data.ALL"
             }
            {:messages (range 50)
             :switch-every 5
             :ports [5672 5673]}))
  (future (produce-and-consume-and-switch 
            {:exchange "analysis-ex-write"
             :queue "analysis-q-0"
             :vhost "VH_ppu"
             :host "localhost"
             :username "analysis"
             :password "analysis"
             :port 5673
             :routing-key "analysis.storedata.ALL"}
            {:messages (range 50)
             :switch-every 5
             :ports [5672 5673]}))
  )

(defn produce [{:keys [exchange routing-key queue username port] :as opts} 
               {:keys [messages sleep-time]}]
  (with-connection [conn (rmq/connect opts)
                    ch (lch/open conn)]
    (infof "producer %10s: switching to instance at port %d" username port)
    (doseq [msg messages]
      (when sleep-time
        (Thread/sleep sleep-time))
      #_(infof "%10s: send %s" username (str msg))
      (lb/publish ch exchange routing-key (str msg)))
    (infof "closing producer %s" username)))

(defn consume [{:keys [exchange routing-key queue username port] :as opts}
               {:keys [prefetch sleep-time message-count]}]
  (with-connection [conn (rmq/connect opts)
                    ch (lch/open conn)]
    (infof "consumer %10s: switching to instance at port %d" username port)
    (let [count-down (java.util.concurrent.CountDownLatch. message-count)] 
      (lb/qos ch prefetch)
      (lc/subscribe ch queue (fn [ch {tag :delivery-tag rk :routing-key} body]
                               (when (rmq/open? ch)
                                 (.countDown count-down)
                                 (lb/ack ch tag)
                                 #_(infof "%10s: recv %s (%s)" username (String. body) rk)
                                 (when sleep-time
                                   (Thread/sleep sleep-time)))) 
                    {:auto-ack false
                     :arguments {"x-priority" 5}})
      (.await count-down)
      (infof "closing consumer %s" username))))

(defn consume-and-switch [{:keys [exchange routing-key queue username] :as opts} 
                          {:keys [switch-every
                                  ports]
                            :as sec}]
  (doseq [port ports]
    (consume (assoc opts :port port)
             (assoc sec :message-count switch-every))))

(defn produce-and-switch [{:keys [exchange routing-key queue username] :as opts} 
                          {:keys [switch-every
                                  ports
                                  messages]
                            :as sec}]
  (doseq [[port msgs] (map vector ports (partition-all switch-every messages))]
    (produce (assoc opts :port port)
             (-> sec
               (assoc :message-count switch-every)
               (assoc :messages msgs)))))

(comment
    (future (produce
            {:exchange "scada-ex-write"
             :queue "scada-q-0"
             :vhost "VH_ppu"
             :host "localhost"
             :username "scada"
             :password "scada"
             :port 5672
             :routing-key "scada.data.ALL"
             }
            {:messages (range 100)
             :sleep-time 5}))
    (future (consume
              {:queue "analysis-q-0"
               :vhost "VH_ppu"
               :host "localhost"
               :username "analysis"
               :password "analysis"
               :port 5673}
              {:prefetch 1
               :sleep-time 200
               :message-count 100}))

(future (consume-and-switch
            {:queue "analysis-q-0"
             :vhost "VH_ppu"
             :host "localhost"
             :username "analysis"
             :password "analysis"}
            {:prefetch 1000
             :sleep-time 200
             :switch-every 25
             :ports [5673 5672 5673 5672]}))

    (future (produce-and-switch
            {:exchange "scada-ex-write"
             :queue "scada-q-0"
             :vhost "VH_ppu"
             :host "localhost"
             :username "scada"
             :password "scada"
             :routing-key "scada.data.ALL"}
            {:messages (range 100)
             :sleep-time 5
             :switch-every 25
             :ports [5672 5673 5672 5673]}))
    
  (time (let [p (future (produce
                  {:exchange "K-ex-write"
                   :queue "K-q-0"
                   :vhost "VH_ppu"
                   :host "localhost"
                   :username "K"
                   :password "K"
                   :port 5672
                   :routing-key "K.10.ALL"
                   }
                  {:messages (range 1000000)}))
      c (future (consume
                  {:queue "UA-q-0"
                   :vhost "VH_ppu"
                   :host "localhost"
                   :username "UA"
                   :password "UA"
                   :port 5672}
                  {:prefetch 100
                   :message-count 1000000}))]
  @p
  @c))
    
    )

