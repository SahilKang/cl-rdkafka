# cl-rdkafka

A Common Lisp wrapper for [librdkafka](https://github.com/edenhill/librdkafka)
via [CFFI](https://common-lisp.net/project/cffi/manual/html_node/index.html).

The project structure is modelled after
[cl-charms](https://github.com/HiTECNOLOGYs/cl-charms):

* The `cl-rdkafka/low-level` package (nicknamed to `cl-rdkafka/ll`)
wraps the
[librdkafka/rdkafka.h](https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.h)
header file.

* And the `cl-rdkafka` package (nicknamed to `kf`) is a higher-level kafka
interface. There's still work to be done for this package to make it more
ergonomic so it's likely to change.

# Usage

It's best to read the [librdkafka](https://github.com/edenhill/librdkafka) docs
when using the `cl-rdkafka/ll` package, and the `test` directory will be
useful, too.

Here are a few examples for the `kf` package:

## Producer

```lisp
;; will add to quicklisp soon, after docstrings are updated
(ql:quickload :cl-rdkafka)

(let ((messages '(("key-1" "value-1") ("key-2" "value-2")))
      (producer (make-instance 'kf:producer
                               :conf (kf:conf
				      "bootstrap.servers" "127.0.0.1:9092")
                               :key-serde #'kf:object->bytes
                               :value-serde #'kf:object->bytes)))
  (loop
     for (k v) in messages
     do (kf:produce producer "topic-name" v :key k))

  (kf:flush producer (* 2 1000)))
```

## Consumer

```lisp
(ql:quickload :cl-rdkafka)

;; see librdkafka and kafka docs for config info

(let* ((string-serde (lambda (x)
                       (kf:bytes->object x 'string)))
       (conf (kf:conf
	      "bootstrap.servers" "127.0.0.1:9092"
	      "group.id" "consumer-group-id"
	      "enable.auto.commit" "false"
	      "auto.offset.reset" "earliest"
	      "offset.store.method" "broker"
	      "enable.partition.eof"  "false"))
       (consumer (make-instance 'kf:consumer
                                :conf conf
                                :key-serde string-serde
                                :value-serde string-serde)))
  (kf:subscribe consumer '("topic-name"))

  (loop
     for message = (kf:poll consumer (* 2 1000))
     while message

     for key = (kf:key message)
     for value = (kf:value message)

     collect (list key value)

     do (kf:value (kf:commit consumer))))

;; => (("key-1" "message-1") ("key-2" "message-2"))
```

# Contributing and Hacking

PRs and GitHub issues are always welcome and feel free to email me with any
questions.

To run the tests (from the project root):

```bash
$ docker-compose -f ./test/docker-compose.test.yml \
  up --build --abort-on-container-exit test
$ docker-compose -f ./test/docker-compose.test.yml down
```

To spin up and teardown a dockerized kafka cluster to hack against:

```bash
# start a cluster on 127.0.0.1:9092
$ docker-compose up --build -d
# tear the cluster down
$ docker-compose down
```
