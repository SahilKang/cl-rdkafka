;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:test/high-level/transactions
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/transactions)

(test multi-topic-commit
  (with-topics ((topic-1 "test-multi-topic-commit-1")
                (topic-2 "test-multi-topic-commit-2"))
    (let ((consumer-1 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "multi-topic-commit-group-1")
                       :serde #'babel:octets-to-string))
          (consumer-2 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "multi-topic-commit-group-2")
                       :serde #'babel:octets-to-string))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "transactional.id" "multi-topic-commit")
                     :serde #'babel:string-to-octets))
          (messages-1 (list "one" "two" "three"))
          (messages-2 (list "four" "five" "six")))
      (kf:subscribe consumer-1 topic-1)
      (kf:subscribe consumer-2 topic-2)
      (sleep 5)
      (kf:initialize-transactions producer 5000)
      (kf:begin-transaction producer)
      (map nil
           (lambda (message)
             (kf:send producer topic-1 message))
           messages-1)
      (map nil
           (lambda (message)
             (kf:send producer topic-2 message))
           messages-2)
      (sleep 5)
      (is (null (loop
                  for message = (kf:poll consumer-1 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-1))))
      (is (null (loop
                  for message = (kf:poll consumer-2 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-2))))
      (kf:commit-transaction producer 5000)
      (is (equal messages-1
                 (loop
                   for message = (kf:poll consumer-1 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer-1))))
      (is (equal messages-2
                 (loop
                   for message = (kf:poll consumer-2 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer-2)))))))

(test multi-topic-abort
  (with-topics ((topic-1 "test-multi-topic-abort-1")
                (topic-2 "test-multi-topic-abort-2"))
    (let ((consumer-1 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "multi-topic-commit-abort-1")
                       :serde #'babel:octets-to-string))
          (consumer-2 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "multi-topic-commit-abort-2")
                       :serde #'babel:octets-to-string))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "transactional.id" "multi-topic-abort")
                     :serde #'babel:string-to-octets))
          (messages-1 (list "one" "two" "three"))
          (messages-2 (list "four" "five" "six")))
      (kf:subscribe consumer-1 topic-1)
      (kf:subscribe consumer-2 topic-2)
      (kf:initialize-transactions producer 5000)
      (kf:begin-transaction producer)
      (map nil
           (lambda (message)
             (kf:send producer topic-1 message))
           messages-1)
      (map nil
           (lambda (message)
             (kf:send producer topic-2 message))
           messages-2)
      (sleep 5)
      (is (null (loop
                  for message = (kf:poll consumer-1 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-1))))
      (is (null (loop
                  for message = (kf:poll consumer-2 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-2))))
      (kf:abort-transaction producer 5000)
      (is (null (loop
                  for message = (kf:poll consumer-1 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-1))))
      (is (null (loop
                  for message = (kf:poll consumer-2 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-2)))))))

(test send-offsets-commit
  (with-topics ((topic-1 "test-send-offsets-commit-1")
                (topic-2 "test-send-offsets-commit-2"))
    (let ((consumer-1 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "send-offsets-commit-1")
                       :serde #'babel:octets-to-string))
          (consumer-2 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "send-offsets-commit-2")
                       :serde #'babel:octets-to-string))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "transactional.id" "send-offsets-commit")
                     :serde #'babel:string-to-octets))
          (messages (list "one" "two" "three")))
      (kf:subscribe consumer-1 topic-1)
      (kf:subscribe consumer-2 topic-2)
      (kf:initialize-transactions producer 5000)
      (kf:begin-transaction producer)
      (map nil
           (lambda (message)
             (kf:send producer topic-1 message))
           messages)
      (sleep 5)
      (is (null (loop
                  for message = (kf:poll consumer-1 5000)
                  while message
                  collect (kf:value message))))
      (kf:commit-transaction producer 5000)
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             for message = (kf:poll consumer-1 5000)
                             while message
                             collect message)))
        (map nil
             (lambda (message)
               (kf:send producer topic-2 (kf:value message)))
             read-messages)
        (kf:send-offsets-to-transaction producer consumer-1 read-messages 5000))
      (is (= cl-rdkafka/ll:rd-kafka-offset-invalid
             (cadar (kf:committed consumer-1 (list (cons topic-1 0)) 5000))))
      (is (null (loop
                  for message = (kf:poll consumer-2 5000)
                  while message
                  collect (kf:value message))))
      (kf:commit-transaction producer 5000)
      (sleep 5)
      (is (= (length messages)
             (cadar (kf:committed consumer-1 (list (cons topic-1 0)) 5000))))
      (is (null (loop
                  for message = (kf:poll consumer-1 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-1))))
      (is (equal messages
                 (loop
                   for message = (kf:poll consumer-2 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer-2)))))))

(test send-offsets-abort
  (with-topics ((topic-1 "test-send-offsets-abort-1")
                (topic-2 "test-send-offsets-abort-2"))
    (let ((consumer-1 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "send-offsets-abort-1")
                       :serde #'babel:octets-to-string))
          (consumer-2 (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*
                                   "enable.auto.commit" "false"
                                   "auto.offset.reset" "earliest"
                                   "group.id" "send-offsets-abort-2")
                       :serde #'babel:octets-to-string))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "transactional.id" "send-offsets-abort")
                     :serde #'babel:string-to-octets))
          (messages (list "one" "two" "three")))
      (kf:subscribe consumer-1 topic-1)
      (kf:subscribe consumer-2 topic-2)
      (kf:initialize-transactions producer 5000)
      (kf:begin-transaction producer)
      (map nil
           (lambda (message)
             (kf:send producer topic-1 message))
           messages)
      (sleep 5)
      (is (null (loop
                  for message = (kf:poll consumer-1 5000)
                  while message
                  collect (kf:value message))))
      (kf:commit-transaction producer 5000)
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             for message = (kf:poll consumer-1 5000)
                             while message
                             collect message)))
        (map nil
             (lambda (message)
               (kf:send producer topic-2 (kf:value message)))
             read-messages)
        (kf:send-offsets-to-transaction producer consumer-1 read-messages 5000))
      (is (= cl-rdkafka/ll:rd-kafka-offset-invalid
             (cadar (kf:committed consumer-1 (list (cons topic-1 0)) 5000))))
      (is (null (loop
                  for message = (kf:poll consumer-2 5000)
                  while message
                  collect (kf:value message))))
      (kf:abort-transaction producer 5000)
      (is (= cl-rdkafka/ll:rd-kafka-offset-invalid
             (cadar (kf:committed consumer-1 (list (cons topic-1 0)) 5000))))
      (kf:seek consumer-1 topic-1 0 0 5000)
      (is (equal messages
                 (loop
                   for message = (kf:poll consumer-1 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer-1))))
      (is (null (loop
                  for message = (kf:poll consumer-2 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer-2)))))))

(test multi-transaction-commit
  (with-topics ((topic "test-multi-transaction-commit"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.auto.commit" "false"
                                 "auto.offset.reset" "earliest"
                                 "group.id" "multi-transaction-commit")
                     :serde #'babel:octets-to-string))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "transactional.id" "multi-transaction-commit")
                     :serde #'babel:string-to-octets))
          (messages-1 (list "one" "two" "three"))
          (messages-2 (list "four" "five" "six"))
          consumed-messages)
      (kf:subscribe consumer topic)
      (kf:initialize-transactions producer 5000)
      (kf:begin-transaction producer)
      (map nil
           (lambda (message)
             (kf:send producer topic message))
           (append messages-1 messages-2))
      (kf:commit-transaction producer 5000)
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             repeat (length messages-1)
                             for message = (kf:poll consumer 5000)
                             for v = (kf:value message)
                             do (setf consumed-messages
                                      (nconc consumed-messages (list v)))
                             collect message)))
        (kf:send-offsets-to-transaction producer consumer read-messages 5000))
      (kf:commit-transaction producer 5000)
      (is (equal consumed-messages messages-1))
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             for message = (kf:poll consumer 5000)
                             while message
                             for v = (kf:value message)
                             do (setf consumed-messages
                                      (nconc consumed-messages (list v)))
                             collect message)))
        (kf:send-offsets-to-transaction producer consumer read-messages 5000))
      (kf:commit-transaction producer 5000)
      (is (equal consumed-messages (append messages-1 messages-2))))))

(test multi-transaction-abort
  (with-topics ((topic "test-multi-transaction-abort"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.auto.commit" "false"
                                 "auto.offset.reset" "earliest"
                                 "group.id" "multi-transaction-abort")
                     :serde #'babel:octets-to-string))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "transactional.id" "multi-transaction-abort")
                     :serde #'babel:string-to-octets))
          (messages-1 (list "one" "two" "three"))
          (messages-2 (list "four" "five" "six"))
          consumed-messages)
      (kf:subscribe consumer topic)
      (kf:initialize-transactions producer 5000)
      (kf:begin-transaction producer)
      (map nil
           (lambda (message)
             (kf:send producer topic message))
           (append messages-1 messages-2))
      (kf:commit-transaction producer 5000)
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             repeat (length messages-1)
                             for message = (kf:poll consumer 5000)
                             for v = (kf:value message)
                             do (setf consumed-messages
                                      (nconc consumed-messages (list v)))
                             collect message)))
        (kf:send-offsets-to-transaction producer consumer read-messages 5000))
      (kf:commit-transaction producer 5000)
      (is (equal consumed-messages messages-1))
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             for message = (kf:poll consumer 5000)
                             while message
                             for v = (kf:value message)
                             do (setf consumed-messages
                                      (nconc consumed-messages (list v)))
                             collect message)))
        (kf:send-offsets-to-transaction producer consumer read-messages 5000))
      (kf:abort-transaction producer 5000)
      (let ((committed (kf:committed consumer (kf:assignment consumer) 5000)))
        (map nil
             (lambda (pair)
               (let ((topic (caar pair))
                     (partition (cdar pair))
                     (offset (cadr pair)))
                 (if (< offset 0)
                     (kf:seek-to-beginning consumer topic partition 5000)
                     (kf:seek consumer topic partition offset 5000))))
             committed))
      (is (equal consumed-messages (append messages-1 messages-2)))
      (kf:begin-transaction producer)
      (let ((read-messages (loop
                             for message = (kf:poll consumer 5000)
                             while message
                             for v = (kf:value message)
                             do (setf consumed-messages
                                      (nconc consumed-messages (list v)))
                             collect message)))
        (kf:send-offsets-to-transaction producer consumer read-messages 5000))
      (kf:commit-transaction producer 5000)
      (is (equal consumed-messages (append messages-1 messages-2 messages-2))))))
