(ns routing.generator.rabbit-password-test
  (:use clojure.test
        routing.generator.rabbit-password))

(deftest hashing-works ; see http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2011-May/012765.html
  (let [salt (byte-array [(.byteValue 0xca) (.byteValue 0xd5) (.byteValue 0x08) (.byteValue 0x9b)])
        name "simon"]
    (is (= "ytUIm8s3AnKsXQjptplKFytfVxI=" (rabbit-password-hash name salt)))))