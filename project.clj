(defproject routing-compiler "0.1.0-SNAPSHOT"
  :description "Automatic contract-based configuration of RabbitMQ"
  :min-lein-version "2.0.0"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-RC1"]
                 [com.novemberain/langohr "3.2.0"
                  :exclude [clj-http]] ; langohr relies on a clj-http version that is not compatible with clojure 1.7
                 [clj-http "1.1.2"] ; compatible with clojure 1.7
                 [clojurewerkz/urly "1.0.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-simple "1.7.12"]
                 [org.clojars.smee/common "1.2.8"]
                 ; web app
                 [compojure "1.3.4"]
                 [ring/ring-jetty-adapter "1.3.2"] ;server
                 [cheshire "5.5.0"] ;json
                 [ring/ring-json "0.3.1"] 
                 [prismatic/schema "0.4.3"] ;data schema description and coercion
                 [rhizome "0.2.5"] ;dot visualization of graphs
                 [com.stuartsierra/component "0.2.3"] ; run different parts as components
                 [metosin/compojure-api "0.21.0"] ; annotate routes with prismatic schema, coerce, validate, generate interactive API documentation
                 [metosin/ring-swagger "0.20.4"] ;fix for transitive dep of compojure-api
                 ]
  :plugins [[lein-set-version "0.4.1"]]
  :profiles {:dev {:dependencies [[spyscope "0.1.5"]]}
             :routing-compiler {:main run
                                :aot [run]}})

