(ns routing.generator.viz-test
  (:use routing.generator.viz
        clojure.test))

(deftest rabbitmq-re-matching
  (are [re s] (re-matches (rabbit->regular-regex re) s)
       "test" "test"
       "test.*" "test.bar"
       "test.#" "test.bar.baz"
       "foo.#.bar" "foo.bar"
       "foo.#.bar" "foo.baz.bar"
       "foo.#.bar" "foo.a.b.c.bar"
       "#" "asdfasfd"
       "#" ""))

(deftest rabbitmq-re-not-matching
  (are [re s] (not (re-matches (rabbit->regular-regex re) s))
       "test" "test1"
       "test" "test.foo"
       "^$" "foo" 
       "test.*" "test.bar.baz"
       "test.*" "test"
       "test.#" "foo"
       "test.#" "test2.foo"
       "foo.#.bar" "foo.baz"
       "foo.#.bar" "foo.baz.bar2"
       "foo.#.bar" "foo2.a.b.c.bar"))
