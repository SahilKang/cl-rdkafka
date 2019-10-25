# cl-rdkafka

[![CircleCI](https://circleci.com/gh/SahilKang/cl-rdkafka.svg?style=shield)](https://circleci.com/gh/SahilKang/cl-rdkafka)
[![tag](https://img.shields.io/github/tag/SahilKang/cl-rdkafka.svg)](https://github.com/SahilKang/cl-rdkafka/tags)
[![quicklisp](http://quickdocs.org/badge/cl-rdkafka.svg)](http://quickdocs.org/cl-rdkafka)
[![license](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://github.com/SahilKang/cl-rdkafka/blob/master/LICENSE)

A Common Lisp wrapper for [librdkafka](https://github.com/edenhill/librdkafka)
via [CFFI](https://common-lisp.net/project/cffi/manual/html_node/index.html)
to allow CL programs to interact with a Kafka cluster.

The project structure is modelled after
[cl-charms](https://github.com/HiTECNOLOGYs/cl-charms):

* The `cl-rdkafka/low-level` package (nicknamed to `cl-rdkafka/ll`)
wraps the
[librdkafka/rdkafka.h](https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.h)
header file.

* And the `cl-rdkafka` package (nicknamed to `kf`) is a higher-level Kafka
interface. There's still work to be done for this package to make it more
ergonomic so it's likely to change.

# Usage

It's best to read the [librdkafka](https://github.com/edenhill/librdkafka) docs
when using the `cl-rdkafka/ll` package, and the `test` directory will be
useful, too.

Here are a few examples for the `kf` package:

## Producer

```lisp
(ql:quickload '(cl-rdkafka babel))

(let* ((serde (lambda (string)
                (babel:string-to-octets string :encoding :utf-8)))
       (messages '(("key-1" "value-1") ("key-2" "value-2")))
       (producer (make-instance
                  'kf:producer
                  :conf (kf:conf
                         "bootstrap.servers" "127.0.0.1:9092")
                  :serde serde)))
  (loop
     for (k v) in messages
     do (kf:produce producer "topic-name" v :key k))

  (kf:flush producer (* 2 1000)))
```

## Consumer

```lisp
(ql:quickload '(cl-rdkafka babel))

;; see librdkafka and kafka docs for config info

(let* ((string-serde (lambda (bytes)
                       (babel:octets-to-string bytes :encoding :utf-8)))
       (conf (kf:conf
              "bootstrap.servers" "127.0.0.1:9092"
              "group.id" "consumer-group-id"
              "enable.auto.commit" "false"
              "auto.offset.reset" "earliest"
              "offset.store.method" "broker"
              "enable.partition.eof"  "false"))
       (consumer (make-instance
                  'kf:consumer
                  :conf conf
                  :serde string-serde)))
  (kf:subscribe consumer '("topic-name"))

  (loop
     for message = (kf:poll consumer (* 2 1000))
     while message

     for key = (kf:key message)
     for value = (kf:value message)

     collect (list key value)

     do (kf:commit consumer)))

;; => (("key-1" "message-1") ("key-2" "message-2"))
```

## Admin

### Create Topics

```lisp
;; client can be either a producer or consumer

(let ((client (make-instance
               'kf:consumer
               :conf (kf:conf
                      "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:create-topic client "your-favorite-topic-name" :partitions 7))

;; => "your-favorite-topic-name"
```

### Delete Topics

```lisp
(let ((client (make-instance
               'kf:consumer
               :conf (kf:conf
                      "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:delete-topic consumer "your-least-favorite-topic-name"))

;; => "your-least-favorite-topic-name"
```

### Create Partitions

```lisp
(let ((client (make-instance
               'kf:consumer
               :conf (kf:conf
                      "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:create-partitions client "needs-moar-partitions-topic-name" 6))

;; => 6
```

### Describe Topic Configs

```lisp
(let ((client (make-instance
               'kf:producer
               :conf (kf:conf
                      "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:describe-config client "mysterious-topic" :topic))

;; => '(("compression.type" . "producer")
;;      ("cleanup.policy" . "delete")
;;         ...
;;         ...
;;      ("flush.ms" . "9223372036854775807")
;;      ("follower.replication.throttled.replicas" . "")
;;      ("message.format.version" . "2.3-IV1")
;;      ("max.message.bytes" . "1000012")
;;      ("message.timestamp.type" . "CreateTime"))
```

### Describe Broker Configs

```lisp
(let ((client (make-instance
               'kf:consumer
               :conf (kf:conf
                      "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:describe-config client "1001" :broker))

;; => '(("advertised.host.name" . "127.0.0.1")
;;      ("offsets.topic.num.partitions" . "50")
;;      ("auto.create.topics.enable" . "true")
;;      ("controller.socket.timeout.ms" . "30000")
;;      ("min.insync.replicas" . "1")
;;         ...
;;         ...
;;      ("replica.fetch.wait.max.ms" . "500")
;;      ("num.recovery.threads.per.data.dir" . "1")
;;      ("ssl.keystore.type" . "JKS")
;;      ("sasl.mechanism.inter.broker.protocol" . "GSSAPI")
;;      ("default.replication.factor" . "1")
;;      ("ssl.truststore.password")
;;      ("log.retention.hours" . "168")
;;      ("advertised.port" . "9092")
;;      ("metrics.recording.level" . "INFO")
;;      ("log.dirs" . "/kafka/kafka-logs-6644432aee02"))
```

### Alter Topic Configs

```lisp
(let ((consumer (make-instance
                 'kf:consumer
                 :conf (kf:conf
                        "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:alter-config consumer
                   "topic-to-alter"
                   '(("message.timestamp.type" . "LogAppendTime")
                     ("cleanup.policy" . "compact"))))
```

### Query Cluster Metadata

```lisp
(let ((consumer (make-instance
                 'kf:consumer
                 :conf (kf:conf
                        "bootstrap.servers" "127.0.0.1:9092"))))
  (kf:cluster-metadata consumer "interesting-topic-name"))

;; => ((:originating-broker . ((:id . 1001)
;;                             (:name . "127.0.0.1:9092/1001")))
;;     (:broker-metadata . (((:id . 1001)
;;                           (:host . "127.0.0.1")
;;                           (:port . 9092))))
;;     (:topic-metadata . (((:topic . "interesting-topic-name")
;;                          (:partitions . (((:id . 0)
;;                                           (:err . nil)
;;                                           (:leader . 1001)
;;                                           (:replicas . (1001))
;;                                           (:in-sync-replicas . (1001)))))
;;                          (:err . nil)))))
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

To spin up and teardown a dockerized Kafka cluster to hack against:

```bash
# start a cluster on 127.0.0.1:9092
$ docker-compose up --build -d
# tear the cluster down
$ docker-compose down
```
