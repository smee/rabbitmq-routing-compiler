(ns routing.generator.rabbit-password
  (:import [org.apache.commons.codec.binary Base64]
           [java.security NoSuchAlgorithmException MessageDigest]))

(defn md5
  "Compute the hex MD5 sum of a byte array."
  [#^bytes b]
  (let [alg (doto (MessageDigest/getInstance "MD5")
              (.reset)
              (.update b))]
    (try
      (.digest alg)
      (catch NoSuchAlgorithmException e
        (throw (new RuntimeException e))))))

(defn rabbit-password-hash 
  "Create a hashed password like RabbitMQ does: Create a 4 byte salt, concatenate the utf8 bytes of the 
password, apply md5 and base64."
  ([^String s] (let [seed (java.lang.System/currentTimeMillis)
                     random (doto (java.security.SecureRandom.) (.setSeed seed))
                     salt (byte-array 4)]
                 (.nextBytes random salt)
                 (rabbit-password-hash s salt)))
  ([^String s ^bytes salt] 
    (let [hashed-bytes (md5 (byte-array (concat salt (.getBytes s))))] 
      (String. (Base64/encodeBase64 (byte-array (concat salt hashed-bytes))) "utf-8"))))