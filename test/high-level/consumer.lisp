;;; Copyright (C) 2018-2019 Sahil Kang <sahil.kang@asilaycomputing.com>
;;;
;;; This file is part of cl-rdkafka.
;;;
;;; cl-rdkafka is free software: you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation, either version 3 of the License, or
;;; (at your option) any later version.
;;;
;;; cl-rdkafka is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with cl-rdkafka.  If not, see <http://www.gnu.org/licenses/>.

(in-package #:cl-user)

(defpackage #:test/high-level/consumer
  (:use #:cl #:1am))

(in-package #:test/high-level/consumer)

(test consumer-subscribe
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092"
                           "group.id" "consumer-subscribe-group"
                           "enable.auto.commit" "true"
                           "auto.offset.reset" "earliest"
                           "offset.store.method" "broker")))
        (expected '("consumer-test-topic" "foobar"))
        actual)
    (kf:subscribe consumer expected)
    (setf actual (sort (kf:subscription consumer) #'string<))
    (kf:unsubscribe consumer)
    (is (and
         (= (length expected) (length actual))
         (every #'string= expected actual)
         (= 0 (length (kf:subscription consumer)))))))

(test poll
  (let* ((bootstrap-servers "kafka:9092")
         (consumer (make-instance
                    'kf:consumer
                    :conf (list "bootstrap.servers" bootstrap-servers
                                "group.id" "consumer-poll-group"
                                "enable.auto.commit" "true"
                                "auto.offset.reset" "earliest"
                                "offset.store.method" "broker")
                    :value-serde (lambda (x)
                                   (babel:octets-to-string x :encoding :utf-8))))
         (expected '("Hello" "World" "!"))
         (topic "consumer-poll-topic"))
    (uiop:run-program
     (format nil "echo -n '~A' | kafkacat -P -D '|' -b '~A' -t '~A'"
             (reduce (lambda (agg s) (format nil "~A|~A" agg s)) expected)
             bootstrap-servers
             topic)
     :force-shell t
     :output nil
     :error-output nil)
    (sleep 2)

    (kf:subscribe consumer (list topic))
    (is (equal expected (loop
                           repeat (length expected)
                           for message = (kf:poll consumer 5000)
                           collect (kf:value message))))))

(test commit
  (let* ((bootstrap-servers "kafka:9092")
         (consumer (make-instance
                    'kf:consumer
                    :conf (list "bootstrap.servers" bootstrap-servers
                                "group.id" "consumer-commit-group"
                                "enable.auto.commit" "false"
                                "auto.offset.reset" "earliest"
                                "offset.store.method" "broker")))
         (expected '(1 2 3))
         (topic "consumer-commit-topic"))
    (uiop:run-program
     (format nil "echo -n 'Live|Laugh|Hack' | kafkacat -P -D '|' -b '~A' -t '~A'"
             bootstrap-servers
             topic)
     :force-shell t
     :output nil
     :error-output nil)
    (sleep 2)

    (kf:subscribe consumer (list topic))
    (is (equal expected (loop
                           repeat (length expected)
                           do
                             (kf:poll consumer 5000)
                             (kf:commit consumer)
                           collect (cadar (kf:committed consumer)))))))

(test assign
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092"
                           "group.id" "consumer-assign-group"
                           "enable.auto.commit" "true"
                           "auto.offset.reset" "earliest"
                           "offset.store.method" "broker")))
        (topic "foobar")
        (partition 35))
    (kf:assign consumer (list (cons topic partition)))
    (destructuring-bind
          (actual-topic . actual-partition) (first (kf:assignment consumer))
      (is (string= topic actual-topic))
      (is (= partition actual-partition)))))

(test consumer-member-id
  (let* ((group "consumer-member-id-group")
         (consumer (make-instance
                    'kf:consumer
                    :conf (list "bootstrap.servers" "kafka:9092"
                                "group.id" group)))
         (topic "consumer-member-id"))
    (is (string= topic (kf:create-topic consumer topic)))
    (sleep 2)
    (kf:subscribe consumer (list topic))
    (sleep 2)

    (let ((member-id (kf:member-id consumer))
          (group-info (kf:group-info consumer group)))
      (is (find member-id
                (cdr (assoc :members group-info))
                :test #'string=
                :key (lambda (alist)
                       (cdr (assoc :id alist))))))))

(test consumer-pause
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092"
                           "group.id" "consumer-pause-group"
                           "auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"
                           "offset.store.method" "broker"
                           "enable.partition.eof" "false")
                   :serde (lambda (bytes)
                            (babel:octets-to-string bytes :encoding :utf-8))))
        (producer (make-instance
                   'kf:producer
                   :conf '("bootstrap.servers" "kafka:9092")
                   :serde (lambda (string)
                            (babel:string-to-octets string :encoding :utf-8))))
        (topic "consumer-pause-topic")
        (good-partition 1)
        (bad-partition 0)
        (messages '("Here" "are" "some" "messages")))
    (is (string= topic (kf:create-topic producer topic :partitions 2)))
    (sleep 2)
    (kf:subscribe consumer (list topic))

    (mapcar (lambda (message)
              (kf:produce producer topic message :partition good-partition))
            messages)
    (mapcar (lambda (message)
              (kf:produce producer topic message :partition bad-partition))
            '("These" "messages" "won't" "be" "consumed"))
    (kf:flush producer 5000)

    (kf:pause consumer (list (cons topic bad-partition)))

    (is (equal messages
               (loop
                  for message = (kf:poll consumer 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer))))))

(test consumer-resume
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092"
                           "group.id" "consumer-resume-group"
                           "auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"
                           "offset.store.method" "broker"
                           "enable.partition.eof" "false")
                   :serde (lambda (bytes)
                            (babel:octets-to-string bytes :encoding :utf-8))))
        (producer (make-instance
                   'kf:producer
                   :conf '("bootstrap.servers" "kafka:9092")
                   :serde (lambda (string)
                            (babel:string-to-octets string :encoding :utf-8))))
        (topic "consumer-resume-topic")
        (good-partition 1)
        (bad-partition 0)
        (goodies '("Here" "are" "some" "messages"))
        (baddies '("Here" "are" "some" "more" "messages")))
    (is (string= topic (kf:create-topic producer topic :partitions 2)))
    (sleep 2)
    (kf:subscribe consumer (list topic))

    (mapcar (lambda (message)
              (kf:produce producer topic message :partition good-partition))
            goodies)
    (mapcar (lambda (message)
              (kf:produce producer topic message :partition bad-partition))
            baddies)
    (kf:flush producer 5000)

    (kf:pause consumer (list (cons topic bad-partition)))
    (is (equal goodies
               (loop
                  for message = (kf:poll consumer 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer))))

    (kf:resume consumer (list (cons topic bad-partition)))
    (is (equal baddies
               (loop
                  for message = (kf:poll consumer 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer))))))

(test query-watermark-offsets
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092")))
        (producer (make-instance
                   'kf:producer
                   :conf '("bootstrap.servers" "kafka:9092")))
        (topic "query-watermark-offsets-topic"))
    (is (string= topic (kf:create-topic producer topic :partitions 2)))
    (sleep 2)

    (is (equal '(0 0) (kf:query-watermark-offsets consumer topic 0)))
    (is (equal '(0 0) (kf:query-watermark-offsets consumer topic 1)))

    (kf:produce producer topic #(2 4) :partition 0)
    (kf:produce producer topic #(1 2) :partition 1)
    (kf:produce producer topic #(3 4) :partition 1)
    (kf:flush producer 5000)

    (is (equal '(0 1) (kf:query-watermark-offsets consumer topic 0)))
    (is (equal '(0 2) (kf:query-watermark-offsets consumer topic 1)))))

(test offsets-for-times
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092"
                           "group.id" "offsets-for-times-group"
                           "auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"
                           "offset.store.method" "broker"
                           "enable.partition.eof" "false")
                   :serde (lambda (bytes)
                            (babel:octets-to-string bytes :encoding :utf-8))))
        (producer (make-instance
                   'kf:producer
                   :conf '("bootstrap.servers" "kafka:9092")
                   :serde (lambda (string)
                            (babel:string-to-octets string :encoding :utf-8))))
        (topic "offsets-for-times-topic"))
    (is (string= topic (kf:create-topic producer topic)))
    (sleep 2)
    (kf:subscribe consumer (list topic))

    (mapcar (lambda (message)
              (kf:produce producer topic message)
              (sleep 1))
            '("There" "is" "a" "delay" "between" "these" "messages"))
    (kf:flush producer 5000)

    (let (delay-offset
          delay-timestamp)
      (loop
         for message = (kf:poll consumer 5000)
         while message
         for delay-message-p = (string= (kf:value message) "delay")

         when delay-message-p
         do (setf delay-offset (kf:offset message)
                  delay-timestamp (kf:timestamp message))

         until delay-message-p)

      (is (= delay-offset
             (cdr (assoc (cons topic 0)
                         (kf:offsets-for-times
                          consumer
                          `(((,topic . 0) . ,delay-timestamp)))
                         :test #'equal))))
      (is (= (1+ delay-offset)
             (cdr (assoc (cons topic 0)
                         (kf:offsets-for-times
                          consumer
                          `(((,topic . 0) . ,(1+ delay-timestamp))))
                         :test #'equal)))))))

(test positions
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf '("bootstrap.servers" "kafka:9092"
                           "group.id" "positions-group"
                           "auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"
                           "offset.store.method" "broker"
                           "enable.partition.eof" "false")
                   :serde (lambda (bytes)
                            (babel:octets-to-string bytes :encoding :utf-8))))
        (producer (make-instance
                   'kf:producer
                   :conf '("bootstrap.servers" "kafka:9092")
                   :serde (lambda (string)
                            (babel:string-to-octets string :encoding :utf-8))))
        (topic "positions-topic"))
    (is (string= topic (kf:create-topic producer topic)))
    (sleep 2)
    (kf:subscribe consumer (list topic))

    (mapcar (lambda (message)
              (kf:produce producer topic message))
            '("Here" "are" "a" "few" "messages"))
    (kf:flush producer 5000)

    (loop
       for i from 0
       for message = (kf:poll consumer 5000)
       while message

       do
         (is (= (1+ i)
                (cdr (assoc (cons topic 0)
                            (kf:positions consumer (list (cons topic 0)))
                            :test #'equal))))
         (kf:commit consumer))))
