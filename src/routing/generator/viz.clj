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


;;;;;;;;;;;;;; experiments with Zach Tellman's Rhizome
(defn- queue? [decl]  
  (= [:declare :queue] ((juxt :action :resource) decl)))

(defn- exchange? [decl] 
  (= [:declare :exchange] ((juxt :action :resource) decl)))

(defn- binding? [decl] 
  (= :bind (:action decl)))

(defn- shovel? [decl]
  (= :shovel (:resource decl)))

(defn- permission? [decl]
  (= :permission (:resource decl)))

(defn- name-of [decl]
  (:name (:arguments decl)))

(defn- rk [decl]
  (-> decl :arguments :routing_key))

(defn ae-name-of [a] 
  (-> a :arguments :arguments :alternate-exchange))

(defn alternate-exchange-binding? [binding]
  (= :alternate-binding (:resource binding)))

;;;;;;;;; functions to query for outgoing bindings and targets of bindings ;;;;;;;;;;;;;;;;;;;;;;
(defn- with-incoming [binding]
  #(with-meta % {:incoming binding}))

(defn- binding-target-of? [binding]
  #(and (= (:vhost binding) (:vhost %))
        (= (-> binding :host :name) (-> % :host :name)) 
        (= (:to binding) (name-of %))
        (= (:resource binding) (:resource %))))

(defn- outgoing-bindings-of 
  "Get all standard bindings starting from the exchange/queue `decl`."
  [bindings decl]
  (bindings [(-> decl :host :name) (:vhost decl) (name-of decl)]))

(defn- direct-children 
  "Find referenced nodes via regular bindings from the given `decl` to any other queue or exchange.
Attaches the binding declaration as meta field `:incoming`."
  [decl bindings directives]
  (let [outgoing-bindings (outgoing-bindings-of bindings decl)]
    (mapcat #(->> directives
               (filter (binding-target-of? %))
               (map (with-incoming %))) outgoing-bindings)))

(defn- alternate-ex-binding-fn 
  "Find binding to an alternate-exchange, if it exists."
  [decl exchanges]
  (when (ae-name-of decl)
    (with-meta 
      (find-where #(and (= (:vhost %) (:vhost decl))
                        (= (-> % :host :name) (-> decl :host :name)) 
                        (= (name-of %) (ae-name-of decl))) 
                  exchanges)
      {:incoming {:resource :alternate-binding
                  :source decl}})))

(defn- host-n-port [uri]
  (let [s (settings-from uri)] 
    (format "%s:%s" (:host s) (:port s))))

(defn- outgoing-shovels [directives queue]
  (->> directives
    (filter shovel?)
    (filter (fn [shovel]
              (let [s (-> shovel :src-uri settings-from)
                    d (-> shovel :dest-uri settings-from)
                    src-loc (host-n-port (:src-uri shovel))
                    src-vhost (:vhost s)]                
                #_(println (= src-vhost (:vhost queue))
                         (= (:src-queue shovel) (name-of queue))
                         (= "localhost" (:host s) (:host d))
                         (= (-> shovel :host :name) (-> queue :host :name))
                         (find-where #(= % src-loc) (-> queue :host :aliases))
                         src-loc
                         (-> queue :host :aliases)) 
                (and (= src-vhost (:vhost queue))
                     (= (:src-queue shovel) (name-of queue))
                     (or
                       ; source and target are local
                       (and 
                         (= "localhost" (:host s) (:host d))
                         (= (-> shovel :host :name) (-> queue :host :name)))
                       ;external target, local queue
                       (if (= "localhost" (:host s))
                         ; local->remote ; FIXME ugly hack: assuming that the upstream user always has the name "upstream"!
                         (or (.contains (:src-queue shovel) "upstream")
                             (.contains (:src-queue shovel) "proxy"))
                         ; remote->local
                         (find-where #(= % src-loc) (-> queue :host :aliases))
                         ))))))))


(defn- shovel-target-of [directives shovel]
  (let [{:keys [src-uri dest-uri dest-exchange host]} shovel
        dest-settings (settings-from dest-uri)
        src-settings (settings-from src-uri)
        dest-vhost (:vhost dest-settings)
        dest-location (host-n-port dest-uri)  
        {shovel-hostname :name ha :aliases} host] 
    (with-meta
      (find-where (fn [target] 
                    (and (= dest-vhost (:vhost target))
                         (exchange? target)
                         (= dest-exchange (name-of target))
                         (or (= shovel-hostname (-> target :host :name)) ;local target
                             (some #{dest-location} (-> target :host :aliases))
                             ; local target, remote source FIXME broken!
                             
                             ))) directives)
        {:incoming shovel})))

(defn- shovel-bindings-fn 
  "Find exchange connected to the given queue (within the same host only, for now)"
  [directives decl]
  (->> decl
    (outgoing-shovels directives)
    (map (partial shovel-target-of directives))))

(defn- permission-of-user [user permissions]
  (find-where #(= user (:user %)) permissions))

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

(defn direct-bindings-between [bindings directives a b]
  (let [outgoing-bindings (outgoing-bindings-of bindings a)]
    (filter #(= b (find-where (binding-target-of? %) directives)) outgoing-bindings)))

(defn path-of-message 
  "Traverse all directives and extract all resources (exchanges, queues, bindings)
that are located on the path of a message that gets send to the `start-node`
with a routing key `routing-key`."
  [adjacent directives start-node routing-key] ;TODO alternate exchange if no outgoing binding matches
  (tree-seq (comp not empty?)  
            (fn [decl] 
              (cond 
                (binding? decl) (filter (binding-target-of? decl) directives)
                (shovel? decl) [(shovel-target-of directives decl)]
                (alternate-exchange-binding? decl) (->> decl
                                                     :source
                                                     adjacent
                                                     (map #(when (alternate-exchange-binding? (:incoming (meta %)))
                                                            %)))
                :else-is-exchange-or-queue 
                (let [targets (adjacent decl)
                      bindings (map (comp :incoming meta) targets)
                      valid-direct-bindings (filter #(and (binding? %) 
                                                          (re-matches (rabbit->regular-regex (rk %)) routing-key)) 
                                                    bindings)
                      ae-bindings (filter alternate-exchange-binding? bindings)
                      shovels (filter shovel? bindings)]
                  (concat shovels
                          valid-direct-bindings
                          ; only add the alternate exchange if there is no shovel and no direct binding where the routing key matches
                          (when (and (empty? shovels) 
                                     (empty? valid-direct-bindings)
                                     (not (queue? decl))) 
                            ae-bindings))))) 
            start-node))

;;;;;;;;;;;;;; graphviz ;;;;;;;;;;;;;;;;;;;;;;;;;
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
   :label [[(name-of decl) "" "" "" "" ""]] 
   :fontcolor "white"
   :style :filled})

(defn ae-binding-style [decl]
  {:style "dotted"
   :color "red"
   :label ""})

(defn shovel-style [decl]
  {:color "red" ;"#FFC0C0" ;light red
   :fontcolor "red" ;"#FFC0C0"
   :style "dashed"
   :arrowhead "oboxobox"
   :label "";(:name decl); as soon as there is a label, dot dies >:(
   })

(defn binding-style [decl]
  {:color "black"
   :fontcolor "black"
   :label (rk decl)   })

(def ^:const not-in-path
  {:color "grey"
   :fontcolor "grey"
   :fillcolor "#EEEEEE"})

(defn vhost-style [^String name]
  {:label name})

(defn routing->graph 
  "Create a dot string for graphviz for the given declarations (refer to routing.generator.io for details of their format)"
  [directives & {:keys [with-ae? with-shovels?
                        start-vhost start-exchange routing-key]
                 :or {with-ae? true with-shovels? true}}] (def directives directives) 
  (let [exchanges (filter exchange? directives)
        queues (filter queue? directives)
        names (->> directives (map (comp :name :host)) distinct) 
        bindings (->> directives
                   (filter binding?)
                   (group-by (juxt (comp :name :host) :vhost :from)))
        adjacent #(if (string? %)
                    nil ;dummy node
                    (concat (direct-children % bindings directives) 
                            (when with-ae? [(alternate-ex-binding-fn % exchanges)]) 
                            (when with-shovels? (shovel-bindings-fn directives %))))
        start (keep #(when (and (= (:vhost %) start-vhost) (= (:name (:arguments %)) start-exchange))
                       %) directives)
        path (if (some nil? [start-vhost start-exchange routing-key])
               (set directives)
               (set (mapcat #(path-of-message adjacent directives % routing-key) start)))
        node->descriptor (fn [decl] 
                           (cond 
                             (= :exchange (:resource decl)) (exchange-style decl) 
                             (= :queue (:resource decl)) (queue-style decl)
                             :else {:color "transparent"
                                    :fillcolor "transparent"
                                    :shape "none"
                                    :width 0
                                    :height 0}))
        edge->descriptor (fn [a b] 
                           (let [binding (-> b meta :incoming)
                                 alt? (alternate-exchange-binding? binding)] 
                             (cond 
                               alt? (ae-binding-style binding) 
                               (= (:vhost a) (:vhost b)) (binding-style binding)
                               :else (shovel-style binding))))]
    (graph->dot (concat exchanges queues names) 
                adjacent
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
                :edge->descriptor #(let [desc (edge->descriptor %1 %2)
                                         binding (-> %2 meta :incoming)] 
                                     (if (contains? path binding)
                                       desc
                                       (merge desc not-in-path))))))

(comment
  (def tracing-msgs
    (read-string (slurp "tracingmsgs.edn")))
  
  (let [start (find-where #(and (= (:vhost %) "VH_ppu") (= (:name (:arguments %)) "winccoa-ex-write")) directives)
        exchanges (filter exchange? directives)
        bindings (->> directives
                   (filter binding?)
                   (group-by (juxt (comp :host :name) :vhost :from)))
        adjacent #(concat (direct-children % bindings directives) 
                          [(alternate-ex-binding-fn % exchanges)] 
                          (shovel-bindings-fn directives %)) ]
  (path-of-message adjacent directives start "winccoa.data.ALL"))
  ) 
