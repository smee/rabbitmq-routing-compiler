(ns run
  (:gen-class))

(declare ^:dynamic port)

(defn -main 
  ([] (-main "5000"))
  ([port]
    ;; use eval to avoid AOT all dependant namespaces
    ;; which takes a while AND creates huge filenames
    ;; which then lead to exceptions like 'java.io.IOException: Die Syntax für den Dateinamen, Verzeichnisnamen oder die Datenträgerbezeichnung ist falsch'
    (eval `(do 
             (require 'schema.core)
             (schema.core/set-fn-validation! true)
             
             (require 'routing.rest.server)             
             (routing.rest.server/start {:port (Integer/parseInt ~port)})))))