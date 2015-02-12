(ns 
  routing.generator.remote-repl
  "simple standalone nrepl server"
  (:require [clojure.tools.nrepl.server :refer [start-server stop-server]]
            [wip.standard-client :as log])
  (:gen-class))

(defn -main [& args]
  (defonce server (start-server :port 7888))
  (log/open-all-connections! log/all-descriptors))


