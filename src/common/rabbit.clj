(ns common.rabbit
  (:require 
    [langohr 
     [core      :as rmq]
     [channel   :as lch]
     [queue     :as lq]
     [consumers :as lc]
     [basic     :as lb]])
  (:import [java.security KeyStore ]
           [javax.net.ssl KeyManagerFactory SSLContext TrustManagerFactory]))

;; test rpc calls
(defn send-rpc 
  "Single shot RPC call: registers an exclusive queue to listen for an answer, sends `payload` to `exchange`
with the given `routing-key`. Returns the payload it receives via the private queue."
  [channel ^String exchange ^String routing-key payload & opts]
  (let [result (promise)
        response-queue (lq/declare-server-named channel :exclusive true)
        correlation-id (str (java.util.UUID/randomUUID))
        opts (-> (apply hash-map opts)
               (assoc :correlation-id correlation-id)
               (assoc :reply-to response-queue))
        consumer-tag correlation-id]
    (future (lc/subscribe channel response-queue 
                          (fn [ch headers payload] {:pre [(= correlation-id (:correlation-id headers))]}
                            (lb/cancel channel consumer-tag)
                            (deliver result payload)) 
                          :auto-ack true
                          :consumer-tag consumer-tag))
    (apply lb/publish channel exchange routing-key payload (flatten (seq opts)))
    @result))


(defn- map->amqp-props [{:keys [^Boolean mandatory ^Boolean immediate ^String content-type ^String ^String content-encoding ^Map headers
                               ^Boolean persistent ^Integer priority ^String correlation-id ^String reply-to ^String expiration ^String message-id
                               ^Date timestamp ^String type ^String user-id ^String app-id ^String cluster-id]
                        :or {mandatory false immediate false}}]
  (.build
    (doto (com.rabbitmq.client.AMQP$BasicProperties$Builder.)
      (.contentType     content-type)
      (.contentEncoding content-encoding)
      (.headers         headers)
      (.deliveryMode    (Integer/valueOf (if persistent 2 1)))
      (.priority        (if priority (Integer/valueOf ^Long priority) nil))
      (.correlationId   correlation-id)
      (.replyTo         reply-to)
      (.expiration      expiration)
      (.messageId       message-id)
      (.timestamp       timestamp)
      (.type            type)
      (.userId          user-id)
      (.appId           app-id)
      (.clusterId       cluster-id))))

(defn send-rpc-via-java [channel exchange routing-key bytes & opts]
  (let [{:keys [time-out] :or {:time-out -1} :as opts} (apply hash-map opts)
        client (com.rabbitmq.client.RpcClient. channel exchange routing-key time-out)
        result (.primitiveCall client (map->amqp-props opts) bytes)]
    (.close client)
    result))

(defmacro with-connection
  "bindings => [name init ...]

  Evaluates body in a try expression with names bound to the values
  of the inits, and a finally clause that calls (langohr.core/close name) on each
  name in reverse order."
  {:added "1.0"}
  [bindings & body]
  (#'clojure.core/assert-args
     (vector? bindings) "a vector for its binding"
     (even? (count bindings)) "an even number of forms in binding vector")
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-connection ~(subvec bindings 2) ~@body)
                                (finally
                                  (langohr.core/close ~(bindings 0)))))
    
    :else (throw (IllegalArgumentException.
                   "with-open only allows Symbols in bindings"))))

(defn create-ssl-context 
  "Constructs an instance of javax.net.ssl.SSLContext. Can then be used to connect to a AMQP-over-SSL connection by
using it as parameter for `langohr.core/connect` with the key `ssl-context`.
`ks-in` and `ts-in` need to be `java.io.InputStream`s, the passwords `ks-password` and `ts-password` are strings."
  [ks-in ks-password ts-in ts-password]
  (let [ks-password (.toCharArray ks-password)
        ts-password (.toCharArray ts-password)
        ks (KeyStore/getInstance "PKCS12")
        kmf (KeyManagerFactory/getInstance "SunX509")
        tks (KeyStore/getInstance "JKS")
        tmf (TrustManagerFactory/getInstance "SunX509")
        c (SSLContext/getInstance "SSLv3")]
    (.load ks ks-in ks-password)
    (.init kmf ks ks-password)
    (.load tks ts-in ts-password)
    (.init tmf tks)
    (.init c (.getKeyManagers kmf) (.getTrustManagers tmf) nil)
    c))