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

(let ((producer (make-instance
                 'kf:producer
                 :conf '("bootstrap.servers" "127.0.0.1:9092")
                 :serde (lambda (string)
                          (babel:string-to-octets string :encoding :utf-8))))
      (messages '(("key-1" "value-1")
                  ("key-2" "value-2"))))
  (loop
     for (k v) in messages
     do (kf:produce producer "topic-name" v :key k))

  (kf:flush producer))
```

## Consumer

```lisp
(ql:quickload '(cl-rdkafka babel))

;; see librdkafka and kafka docs for config info

(let ((consumer (make-instance
                 'kf:consumer
                 :conf '("bootstrap.servers" "127.0.0.1:9092"
                         "group.id" "consumer-group-id"
                         "enable.auto.commit" "false"
                         "auto.offset.reset" "earliest"
                         "offset.store.method" "broker"
                         "enable.partition.eof"  "false")
                 :serde #'babel:octets-to-string)))
  (kf:subscribe consumer '("topic-name"))

  (loop
     for message = (kf:poll consumer 2000)
     while message

     for key = (kf:key message)
     for value = (kf:value message)

     collect (list key value)

     do (kf:commit consumer)))

;; => (("key-1" "message-1") ("key-2" "message-2"))
```

## Admin :construction:

The admin api is not publicly exposed because it's still being tweaked.
The [admin functionality](src/high-level/admin/) is still accessible if needed
(see [tests](test/high-level/admin.lisp) for usage examples), but it will be
changing significantly in the near future.

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
