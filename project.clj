(defproject routing-compiler "0.1.0-SNAPSHOT"
  :description "Automatic contract-based configuration of RabbitMQ"
  :min-lein-version "2.0.0"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.novemberain/langohr "3.0.1"]
                 [clojurewerkz/urly "1.0.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-simple "1.7.10"]
                 [org.clojars.smee/common "1.2.8-SNAPSHOT"]
                 ; web app
                 [compojure "1.3.1"]
                 [ring/ring-jetty-adapter "1.3.2"] ;server
                 [liberator "0.12.2"] ;rest
                 [cheshire "5.4.0"] ;json
                 [ring/ring-json-patch "0.2.0"] ;wrap-json-params, local version with applied patch from pull request 11: enable on-error callback for malformed json
                 [prismatic/schema "0.3.7"] ;data schema description and coercion
                 [rhizome "0.2.1"] ;dot visualization
                 ]
  :profiles {:dev {:dependencies [[spyscope "0.1.5"]]}
             :routing {:main controlchannel.service
                       :aot [controlchannel.service]}
             :routing-compiler {:main routing.routing-rest
                                :aot [routing.routing-rest]}})
