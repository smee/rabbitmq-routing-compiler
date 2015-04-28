(ns ^{:doc "render current routing structures"} 
routing.generator.viz
  (:require [clojure.java.shell :as sh]
            [org.clojars.smee.seq :refer [find-where distinct-by]] 
            [rhizome.viz :refer [view-graph]]
            [rhizome.dot :refer [graph->dot]] 
            [langohr.core :refer [settings-from]]))

(defn dot->png 
  "Render a dot format string to a png. Returns byte array with the image."
  [^String dot]
  (:out (sh/sh "dot" "-Tpng" :in dot :out-enc :bytes)))


(defn- queue? [{r :resource}]  
  (= :queue r))

(defn- exchange? [{r :resource}] 
  (= :exchange r))

(defn- name-of [decl]
  (-> decl :arguments :name))

(defn- rk [decl]
  (-> decl :arguments :routing_key))

;;;;;;;;;;;;;; tracing of a message's path through rabbit

(defn rabbit->regular-regex 
  "Convert a rabbitmq binding expression into a java regular expression."
  [^String ra-re]
  (-> ra-re
    (.replaceAll "\\." "\\\\.")
    (.replaceAll "\\*" "[^.]+")
    (.replaceAll "\\\\.#\\\\." "(?:\\\\..+\\\\.|\\\\.)")
    (.replaceAll "^#\\\\." "(?:.+\\.)?")
    (.replaceAll "\\\\.#$" "(?:\\\\..+)?")
    (.replaceAll "#" ".*")
    re-pattern))

(defn should-follow-edge? [node routing-key {r :resource :as edge}]
  (or
    (nil? routing-key) 
    (and (= :exchange-binding r) (re-matches (rabbit->regular-regex (rk edge)) routing-key)) 
    (and (= :queue-binding r) (re-matches (rabbit->regular-regex (rk edge)) routing-key))
    (= :shovel r))
  )

(defn transitive-closure-of 
  "Traverse all directives and extract all resources (exchanges, queues, bindings)
that are located on the path of a message that gets send to the `start-node`
with a routing key `routing-key`."
  [nodes edges start-node routing-key]
  (let [graph (apply merge-with merge 
                  (for [[n1 targets] edges, [n2 edges] targets, edge edges]
                    {n1 {edge n2}}))]
    (loop [visited #{}, hull #{start-node}, to-visit #{start-node}]
      (if (empty? to-visit)
        hull
        (let [node (first to-visit)
              to-visit (disj to-visit node)
              potential-edges (get graph node)
              real-edges (filter (partial should-follow-edge? node routing-key) (keys potential-edges))
              real-edges (if (empty? real-edges) 
                           [(find-where #(= :alternate-exchange-binding (:resource %)) (keys potential-edges))]
                           real-edges)]
          ; TODO fanout and headers exchange
          ; TODO alternate exchange if no other routing occurs
          ; TODO DLX for queues
          ; TODO shovels may rename routing-key
          (recur (conj visited node) 
                 (apply conj hull node real-edges) 
                 (if potential-edges 
                   (into to-visit (map potential-edges real-edges))
                   to-visit)))))))

;;;;;;;;;;;;;; graphviz ;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- escape-label [^String lbl]
  (-> lbl
    (.replaceAll ">" "\\\\>")
    (.replaceAll "<" "\\\\<")
    (.replaceAll " " "\\\\ ")))

(defn obj->color [obj] 
  (str "#" (subs (Integer/toString (hash obj) 16) 0 6)))

(defn exchange-style [decl]
  {:fillcolor "blue" ;(obj->color (:vhost decl));(if (:internal (:arguments decl)) "grey" "blue")
   :fontcolor "white"
   :style :filled
   :label (name-of decl)
   :shape "ellipse"})

(defn queue-style [decl]
  {:fillcolor "red" ;(obj->color (:vhost decl))
   :shape "record"
   :label [[(escape-label (name-of decl)) "" "" ""]] 
   :fontcolor "white"
   :style :filled})

(defn ae-binding-style [_]
  {:style "dotted"
   :color "red"
   :label ""})

(defn dlx-binding-style [_]
  {:style "dotted"
   :color "red"
   :label ""})

(defn shovel-style [decl]
  {:color "red" ;"#FFC0C0" ;light red
   :fontcolor "red" ;"#FFC0C0"
   :style "dashed"
   :arrowhead "oboxobox"
   :label (:name decl)
   })

(defn binding-style [decl]
  {:color "black"
   :fontcolor "black"
   :label (rk decl)})

(def ^:const not-in-path
  {:color "grey"
   :fontcolor "grey"
   :fillcolor "#EEEEEE"})

(defn vhost-style [^String name]
  {:label name})

;;;;;;;;;;;;;;;;;; create graphs ;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- alternate-exchange-binding? [decl]
  (-> decl :arguments :arguments :alternate-binding))

(defn- dead-letter-binding? [decl]
  (-> decl :arguments :arguments :x-dead-letter-exchange))

(defn extract-alternate-exchange-bindings [exchanges]
  (for [e exchanges :when (-> e :arguments :arguments :alternate-exchange)]
    {:resource :alternate-exchange-binding
     :vhost (:vhost e)
     :host (:host e)
     :from (name-of e)
     :to (-> e :arguments :arguments :alternate-exchange)}))

(defn extract-dlx-bindings [queues]
  (for [q queues :when (-> q :arguments :arguments :x-dead-letter-exchange)]
    {:resource :dlx-binding
     :vhost (:vhost q)
     :host (:host q)
     :from (name-of q)
     :to (-> q :arguments :arguments :x-dead-letter-exchange)}))

(defn- create-adjacency-matrix 
  "Create a map of node to map of node to edge."
  [directives]
  (let [f #(into {} 
                 (comp (filter %) 
                       (map (juxt (juxt name-of :vhost) identity)))
                 directives)
        exchanges (f exchange?)
        queues (f queue?)
        directives (concat directives 
                           (extract-alternate-exchange-bindings directives) 
                           (extract-dlx-bindings directives))] 
    (reduce (fn [m {:keys [resource from to vhost] :as d}]
              (let [[a b] 
                    (condp = resource
                      :exchange-binding [(exchanges [from vhost]) (exchanges [to vhost])] 
                      :queue-binding [(exchanges [from vhost]) (queues [to vhost])]
                      :alternate-exchange-binding [(exchanges [from vhost]) (exchanges [to vhost])]
                      :dlx-binding [(queues [from vhost]) (exchanges [to vhost])]
                      :shovel [(queues [(:src-queue d) (-> d :src-uri settings-from :vhost)]) (exchanges [(:dest-exchange d) (-> d :dest-uri settings-from :vhost)])]
                      nil)]
                (if (and a b)
                  (update-in m [a b] (fnil conj []) d)
                  m))) 
            {} directives)))

(defn routing->graph 
  "Create a dot string for graphviz for the given declarations 
(refer to routing.generator.features for details of their format)"
  [directives {:keys [start-vhost 
                      start-exchange 
                      routing-key]}] (def directives directives) 
  (org.clojars.smee.util/def-let [exchanges (filter exchange? directives)
        queues (filter queue? directives)
        hosts (->> directives (map (comp :name :host)) distinct)
        edges (create-adjacency-matrix directives) 
        start (some #(when (and (= (:vhost %) start-vhost)
                                (exchange? %)
                                (= (name-of %) start-exchange))
                       %) directives)
        path (if (and start-vhost start-exchange)
               (transitive-closure-of (concat exchanges queues) edges start routing-key)
               (set (concat directives (for [[_ m] edges, [_ edge] m] edge))))
        node->descriptor (fn [decl] 
                           (cond 
                             (exchange? decl) (exchange-style decl) 
                             (queue? decl) (queue-style decl)
                             :else {:color "transparent"
                                    :fillcolor "transparent"
                                    :shape "none"
                                    :width 0
                                    :height 0}))
        edge->descriptor (fn [{r :resource :as b}] 
                           (cond 
                             (= r :alternate-exchange-binding) (ae-binding-style b)
                             (= r :dlx-binding) (dlx-binding-style b)
                             (= r :shovel) (shovel-style b)
                             :else (binding-style b)))] 
    (graph->dot (concat exchanges queues hosts) 
                (comp keys edges) 
                :directed? true
                :vertical? false
                :options {:bgcolor  :transparent
                          :truecolor true
                          :rankdir "LR"} 
                :node->cluster (fn [decl] (if (string? decl) 
                                            decl 
                                            [(-> decl :host :name) (:vhost decl)]))
                :cluster->parent (fn [cluster] (if (coll? cluster) (first cluster) nil)) 
                :cluster->descriptor (fn [cluster]
                                       (cond 
                                         (string? cluster) (vhost-style cluster)
                                         (some #(= cluster [(-> % :host :name) (:vhost %)]) path) (vhost-style (second cluster))
                                         :else (merge (vhost-style (second cluster)) not-in-path)))
                :node->descriptor #(let [desc (node->descriptor %)] 
                                     (if (contains? path %)
                                       desc
                                       (merge desc not-in-path)))
                :edge->descriptor (fn [a b] 
                                    (vec
                                      (for [binding (get-in edges [a b])
                                            :let [desc (edge->descriptor binding)]] 
                                        (if (contains? path binding)
                                          desc
                                          (merge desc not-in-path))))))))