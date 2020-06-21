# cl-rdkafka

[![CircleCI](https://circleci.com/gh/SahilKang/cl-rdkafka.svg?style=shield)](https://circleci.com/gh/SahilKang/cl-rdkafka)
[![tag](https://img.shields.io/github/tag/SahilKang/cl-rdkafka.svg)](https://github.com/SahilKang/cl-rdkafka/tags)
[![quicklisp](http://quickdocs.org/badge/cl-rdkafka.svg)](http://quickdocs.org/cl-rdkafka)
[![license](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://github.com/SahilKang/cl-rdkafka/blob/master/LICENSE)

A Common Lisp client library for [Apache Kafka](https://kafka.apache.org/).

The public API is split between two packages:

* **cl-rdkafka/low-level**

  Nicknamed `cl-rdkafka/ll`, this package provides
  [CFFI](https://common-lisp.net/project/cffi/) bindings for
  [librdkafka](https://github.com/edenhill/librdkafka).

* **cl-rdkafka**

  Nicknamed `kf`, this package provides a higher-level interface :nail_care:
  with amenities such as garbage-collection :recycle:, out-of-band
  error processing :leftwards_arrow_with_hook:, and more!

Documentation for `cl-rdkafka/ll` can be found in
[librdkafka/rdkafka.h](https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.h),
and `kf` is documented under the [API section](#API).

# Examples

## Producer

```lisp
(ql:quickload '(cl-rdkafka babel))

(let ((producer (make-instance
                 'kf:producer
                 :conf '("bootstrap.servers" "127.0.0.1:9092"
                         "enable.idempotence" "true")
                 :serde #'babel:string-to-octets))
      (messages '(("key-1" "value-1")
                  ("key-2" "value-2"))))
  (loop
     for (k v) in messages
     do (kf:send producer "topic-name" v :key k))

  (kf:flush producer))
```

## Consumer

```lisp
(ql:quickload '(cl-rdkafka babel))

(let ((consumer (make-instance
                 'kf:consumer
                 :conf '("bootstrap.servers" "127.0.0.1:9092"
                         "group.id" "consumer-group-id"
                         "enable.auto.commit" "false"
                         "auto.offset.reset" "earliest")
                 :serde #'babel:octets-to-string)))
  (kf:subscribe consumer "topic-name")

  (loop
     for message = (kf:poll consumer 2000)
     while message

     for key = (kf:key message)
     for value = (kf:value message)

     collect (list key value)

     do (kf:commit consumer)))

;; => (("key-1" "message-1") ("key-2" "message-2"))
```

# Contributing and Hacking

PRs and GitHub issues are always welcome :octocat: and feel free to email me
with any questions :incoming_envelope:

To run the tests:

```bash
$ docker-compose -f ./test/docker-compose.test.yml \
>   up --build --remove-orphans --abort-on-container-exit test

$ docker-compose -f ./test/docker-compose.test.yml down --rmi all
$ docker system prune -fa && docker volume prune -f
```

To spin up and teardown a dockerized Kafka cluster to hack against:

```bash
# start a cluster on 127.0.0.1:9092
$ docker-compose up --build --remove-orphans -d

# tear the cluster down
$ docker-compose down --rmi all

# clean up after yourself
$ docker system prune -fa && docker volume prune -f
```

# API

* [producer class](#producer-class)
    * [send](#send)
    * [flush](#flush)
* [consumer class](#consumer-class)
    * [poll](#poll)
    * [subscribe](#subscribe)
    * [unsubscribe](#unsubscribe)
    * [subscription](#subscription)
    * [assign](#assign)
    * [assignment](#assignment)
    * [commit](#commit)
    * [committed](#committed)
    * [pause](#pause)
    * [resume](#resume)
    * [member-id](#member-id)
    * [offsets-for-times](#offsets-for-times)
    * [watermarks](#watermarks)
    * [positions](#positions)
    * [close](#close)
* [message class](#message-class)
    * [key](#key)
    * [value](#value)
    * [topic](#topic)
    * [partition](#partition)
    * [offset](#offset)
    * [timestamp](#timestamp)
    * [headers](#headers)
* [future class](#future-class)
    * [value](#value-1)
    * [then](#then)
    * [donep](#donep)
* [conditions](#conditions)
    * [kafka-error](#kafka-error)
    * [rdkafka-error](#rdkafka-error)
    * [partition-error](#partition-error)
    * [partial-error](#partial-error)
    * [allocation-error](#allocation-error)
* [admin api :construction:](#admin-api)

## producer class

A client that produces messages to kafka topics.

`make-instance` accepts the following keyword args:

* `conf`

   A required plist, alist, or hash-table mapping config keys to their
   respective values; both keys and values should be strings. The provided
   key-value pairs are passed as-is to librdkafka, so consult the
   [librdkafka config docs](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
   for more info.

* `serde`

  An optional unary function accepting an object and returning a byte
  sequence; defaults to `#'identity`.

* `key-serde`

  An optional unary function used to serialize message keys; defaults
  to `serde`.

* `value-serde`

  An optional unary function used to serialize message values; defaults
  to `serde`.

Example:

```lisp
(let ((producer (make-instance
                 'kf:producer
                 :conf '("bootstrap.servers" "127.0.0.1:9092"
                         "enable.idempotence" "true")
                 :serde #'babel:string-to-octets))
      (messages '(("key-1" "value-1")
                  ("key-2" "value-2"))))
  (loop
     for (k v) in messages
     do (kf:send producer "topic-name" v :key k))

  (kf:flush producer))
```

---

### send

```lisp
((producer producer) (topic string) value &key key partition headers timestamp)
```

Asynchronously send a message and return a `message` `future`.

If `partition` is not specified, one is chosen using the `topic`'s
partitioner function.

If specified, `headers` should be an alist mapping strings to
byte-vectors.

`timestamp` is the number of milliseconds since the UTC epoch. If not
specified, one will be generated by this call.

May signal `partition-error` or condition from `producer`'s serde. A
`store-function` restart will be provided if it's a serde condition.

---

### flush

```lisp
((producer producer))
```

Block while in-flight messages are sent to kafka cluster.

---

## consumer class

A client that consumes messages from kafka topics.

`make-instance` accepts the following keyword args:

* `conf`

  A required plist, alist, or hash-table mapping config keys to their
  respective values; both keys and values should be strings. The provided
  key-value pairs are passed as-is to librdkafka, so consult the
  [librdkafka config docs](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
  for more info.

* `serde`

  An optional unary function accepting a byte vector and returning a
  deserialized value; defaults to `#'identity`.

* `key-serde`

  An optional unary function used to deserialize message keys; defaults
  to `serde`.

* `value-serde`

  An optional unary function used to deserialize message values; defaults
  to `serde`.

Example:

```lisp
(let ((consumer (make-instance
                 'kf:consumer
                 :conf '("bootstrap.servers" "127.0.0.1:9092"
                         "group.id" "consumer-group-id"
                         "enable.auto.commit" "false"
                         "auto.offset.reset" "earliest")
                 :serde #'babel:octets-to-string)))
  (kf:subscribe consumer "topic-name")

  (loop
     for message = (kf:poll consumer 2000)
     while message

     for key = (kf:key message)
     for value = (kf:value message)

     collect (list key value)

     do (kf:commit consumer)))
```

---

### poll

```lisp
((consumer consumer) (timeout-ms integer))
```

Block for up to `timeout-ms` milliseconds and return a `message` or nil.

May signal `partition-error` or condition from `consumer`'s serde. A
`store-function` restart will be provided if it's a serde condition.

---

### subscribe

#### sequence specialization

```lisp
((consumer consumer) (topics sequence))
```

Subscribe `consumer` to `topics`.

Any topic prefixed with `^` will be regex-matched with the cluster's topics.

#### string specialization

```lisp
((consumer consumer) (topic string))
```

Subscribe `consumer` to `topic`.

If `topic` starts with `^`, then it will be regex-matched with the cluster's
topics.

---

### unsubscribe

```lisp
((consumer consumer))
```

Unsubscribe `consumer` from its current topic subscription.

---

### subscription

```lisp
((consumer consumer))
```

Return a list of topic names that `consumer` is subscribed to.

---

### assign

```lisp
((consumer consumer) (partitions sequence))
```

Assign `partitions` to `consumer`.

`partitions` should be a sequence of `(topic . partition)` cons cells.

---

### assignment

```lisp
((consumer consumer))
```

Return a `(topic . partition)` list of partitions assigned to `consumer`.

---

### commit

```lisp
((consumer consumer) &key offsets asyncp)
```

Commit `offsets` to broker.

If `offsets` is nil, then the current assignment is committed;
otherwise, `offsets` should be an alist mapping `(topic . partition)` cons
cells to either `(offset . metadata)` cons cells or lone offset values.

On success, an alist of committed offsets is returned, mapping
`(topic . partition)` to `(offset . metadata)`.

On failure, either an `rdkafka-error` or `partial-error` is signalled.
The `partial-error` will have the slots:
  * `goodies`: Same format as successful return value
  * `baddies`: An alist mapping `(topic . partition)` to `rdkafka-error`

If `asyncp` is true, then a `future` will be returned instead.

---

### committed

```lisp
((consumer consumer) (partitions sequence) (timeout-ms integer))
```

Block for up to `timeout-ms` milliseconds and return committed offsets
for `partitions`.

`partitions` should be a sequence of `(topic . partition)` cons cells.

On success, an alist of committed offsets is returned, mapping
`(topic . partition)` to `(offset . metadata)`.

On failure, either an `rdkafka-error` or `partial-error` is signalled.
The `partial-error` will have the slots:
  * `goodies`: Same format as successful return value
  * `baddies`: An alist mapping `(topic . partition)` to `rdkafka-error`

---

### pause

```lisp
((consumer consumer) (partitions sequence))
```

Pause consumption from `partitions`.

`partitions` should be a sequence of `(topic . partition)` cons cells.

`partitions` is returned on success.

On failure, either an `rdkafka-error` or `partial-error` is signalled.
The `partial-error` will have the slots:
  * `goodies`: A list of `(topic . partition)` cons cells
  * `baddies`: An alist mapping `(topic . partition)` to `rdkafka-error`

---

### resume

```lisp
((consumer consumer) (partitions sequence))
```

Resume consumption from `partitions`.

`partitions` should be a sequence of `(topic . partition)` cons cells.

`partitions` is returned on success.

On failure, either an `rdkafka-error` or `partial-error` is signalled.
The `partial-error` will have the slots:
  * `goodies`: A list of `(topic . partition)` cons cells
  * `baddies`: An alist mapping `(topic . partition)` to `rdkafka-error`

---

### member-id

```lisp
((consumer consumer))
```

Return `consumer`'s broker-assigned group member-id.

---

### offsets-for-times

```lisp
((consumer consumer) (timestamps list) (timeout-ms integer))
```

Look up the offsets for the given partitions by timestamp.

The returned offset for each partition is the earliest offset whose
timestamp is greater than or equal to the given timestamp in `timestamps`.

`timestamps` should be an alist mapping `(topic . partition)` cons cells
to timestamp values.

On success, an alist of offsets is returned, mapping `(topic . partition)`
cons cells to offset values.

On failure, either an `rdkafka-error` or `partial-error` is signalled.
The `partial-error` will have the slots:
  * `goodies`: Same format as successful return value
  * `baddies`: An alist mapping `(topic . partition)` to `rdkafka-error`

---

### watermarks

```lisp
((consumer consumer) (topic string) (partition integer) (timeout-ms integer))
```

Query broker for low (oldest/beginning) and high (newest/end) offsets.

A `(low . high)` cons cell is returned.

---

### positions

```lisp
((consumer consumer) (partitions sequence))
```

Retrieve current positions (offsets) for `partitions`.

`partitions` should be a sequence of `(topic . partition)` cons cells.

On success, an alist of positions is returned, mapping
`(topic . partition)` to one of either:
  * 1 plus the last consumed message offset
  * nil if there was no previous message.

On failure, either an `rdkafka-error` or `partial-error` is signalled.
The `partial-error` will have the slots:
  * `goodies`: Same format as successful return value
  * `baddies`: An alist mapping `(topic . partition)` to `rdkafka-error`

---

### close

```lisp
((consumer consumer))
```

Close `consumer` after revoking assignment, committing offsets,
and leaving group.

`consumer` will be closed during garbage collection if it's still open;
this method is provided if closing needs to occur at a well-defined
time.

---

## message class

A kafka message as returned by `consumer`'s `poll` or `producer`'s `send`.

`make-instance` should not be called with this class.

Example:

```lisp
(let ((message (kf:poll consumer 5000)))
  (kf:key message)
  ;; => "key-1", #(107 101 121 45 49)

  (kf:value message)
  ;; => "Hello", #(72 101 108 108 111)

  (kf:topic message)
  ;; => "foobar"

  (kf:partition message)
  ;; => 0

  (kf:offset message)
  ;; => 0

  (kf:timestamp message)
  ;; => 1577002478269, :CREATE-TIME

  (kf:headers message)
  ;; => '(("one" . #(1 2 3))
  ;;      ("two" . #(4 5 6)))

  )
```

---

### key

```lisp
((message message))
```

Return `(values deserialized-key serialized-key)` from `message`.

---

### value

```lisp
((message message))
```

Return `(values deserialized-value serialized-value)` from `message`.

---

### topic

```lisp
((message message))
```

Return the topic `message` originated from.

---

### partition

```lisp
((message message))
```

Return the partition `message` originated from.

---

### offset

```lisp
((message message))
```

Return the offset for `message`.

---

### timestamp

```lisp
((message message))
```

Return `(values timestamp timestamp-type)` from `message`.

If timestamp is not available, then nil is returned. Otherwise:
  * `timestamp` is the number of milliseconds since the UTC epoch
  * `timestamp-type` is either `:create-time` or `:log-append-time`

---

### headers

```lisp
((message message))
```

Return headers from `message` as an alist mapping strings to byte vectors.

---

## future class

A future to hold the result of an async operation.

`make-instance` should not be called with this class.

Example:

```lisp
(let ((future (kf:send producer "topic" "message")))
  (kf:donep future) ;; => nil
  (kf:value future) ;; => #<MESSAGE {1005BE9D23}>
  (kf:donep future) ;; => t

  (let ((new-future (kf:then future
                             (lambda (message err)
                               (when err
                                 (error err))
                               (kf:value message)))))
    (kf:value new-future))) ;; => "message"
```

---

### value

```lisp
((future future))
```

Wait until `future` is done and return its value or signal its condition.

---

### then

```lisp
((future future) (callback function))
```

Return a new `future` that calls `callback` when current future completes.

`callback` should be a binary function accepting the positional args:
  1. `value`: the value that the current future evaluates to, or nil
              when it signals a condition.
  2. `condition`: the condition signalled by the current future, or nil
                  when it does not signal a condition.

`callback` is called in a background thread.

---

### donep

```lisp
((future future))
```

Determine if `future` is done processing.

---

## conditions

The conditions are structured in the following class hierarchy:

* `cl:serious-condition`
  * `cl:storage-condition`
    * `allocation-error`
  * `cl:error`
    * `kafka-error`
      * `rdkafka-error`
        * `partition-error`
      * `partial-error`

---

### kafka-error

Generic condition signalled by cl-rdkafka for expected errors.

Slot readers:
* `description`: Hopefully some descriptive description describing the error.

---

### rdkafka-error

Condition signalled for librdkafka errors.

Slot readers:
* `enum`: `cl-rdkafka/ll:rd-kafka-resp-err` enum symbol.
* `description`: `enum` description (inherited)

---

### partition-error

Condition signalled for errors specific to a topic's partition.

Slot readers:
* `topic`: Topic name
* `partition`: Topic partition
* `enum`: `cl-rdkafka/ll:rd-kafka-resp-err` enum symbol (inherited)
* `description`: `enum` description (inherited)

---

### partial-error

Condition signalled for operations that partially failed.

Slot readers:
* `goodies`: Successful results
* `baddies`: Unsuccessful results
* `description`: `baddies` description (inherited)

---

### allocation-error

Condition signalled when librdkafka functions fail to allocate pointers.

Slot readers:
* `name`: Name of the object that failed to be allocated.
* `description`: Details about why the allocation may have failed.

---

## Admin API

The admin API is still baking :bread:, so it's not publicly exposed.
The [admin functionality](src/high-level/admin/) is accessible if needed
(see [tests](test/high-level/admin.lisp) for usage examples), but it will be
changing significantly in the near future.
