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

(defvar *conf* (kf:conf
                "bootstrap.servers" "kafka:9092"
                "group.id" (write-to-string (get-universal-time))
                "enable.auto.commit" "true"
                "auto.offset.reset" "earliest"
                "offset.store.method" "broker"))

(test consumer-subscribe
  (setf (gethash "group.id" *conf*) (write-to-string (get-universal-time)))
  (let ((consumer (make-instance 'kf:consumer :conf *conf*))
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
  (setf (gethash "group.id" *conf*) (write-to-string (get-universal-time)))
  (let ((consumer (make-instance 'kf:consumer
                                 :conf *conf*
                                 :value-serde
                                 (lambda (x)
                                   (babel:octets-to-string x :encoding :utf-8))))
        (expected '("Hello" "World" "!"))
        (actual (make-array 3 :element-type 'string :initial-element "")))
    (kf:subscribe consumer '("consumer-test-topic"))

    (loop
       for i below 3
       for message = (kf:poll consumer 5000)
       for value = (kf:value message)
       do (setf (elt actual i) value))

    (is (and
         (= (length expected) (length actual))
         (every #'string= expected actual)))))

(test commit
  (setf (gethash "enable.auto.commit" *conf*) "false"
        (gethash "group.id" *conf*) "commit-test-group")

  (let ((consumer (make-instance 'kf:consumer :conf *conf*))
        (expected '(1 2 3))
        actual
        (commits (make-array 3)))
    (kf:subscribe consumer '("consumer-test-topic"))

    (loop
       for i below 3
       do (kf:poll consumer 5000)
       do (kf:commit consumer)
       do (setf (elt commits i) (kf:committed consumer)))

    ;; each element of commits is a vector of one element
    (let ((flattened (map 'list (lambda (v) (elt v 0)) commits)))
      (setf actual (mapcar (lambda (t+p) (kf:offset t+p)) flattened)))

    (is (and
         (= (length expected) (length actual) (length commits))
         (every #'= expected actual)
         (apply #'= (map 'list #'length commits))))))

(test assign
  (let ((consumer (make-instance 'kf:consumer :conf *conf*))
        assignment)
    (kf:assign consumer (list (make-instance 'kf:topic+partition
                                             :topic "foobar"
                                             :offset 7
                                             :partition 35
                                             :metadata #(4 8 12))))

    (setf assignment (elt (kf:assignment consumer) 0))

    (is (and
         (string= "foobar" (kf:topic assignment))
         (= 7 (kf:offset assignment))
         (= 35 (kf:partition assignment))
         (equalp #(4 8 12) (kf:metadata assignment))))))

(test consumer-member-id
  (let* ((group "consumer-member-id-group")
         (consumer (make-instance
                    'kf:consumer
                    :conf (kf:conf
                           "bootstrap.servers" "kafka:9092"
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
                   :conf (kf:conf
                          "bootstrap.servers" "kafka:9092"
                          "group.id" "consumer-pause-group"
                          "auto.offset.reset" "earliest"
                          "enable.auto.commit" "false"
                          "offset.store.method" "broker"
                          "enable.partition.eof" "false")
                   :serde (lambda (bytes)
                            (babel:octets-to-string bytes :encoding :utf-8))))
        (producer (make-instance
                   'kf:producer
                   :conf (kf:conf
                          "bootstrap.servers" "kafka:9092")
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
    (kf:flush producer 2000)

    (kf:pause consumer (list (cons topic bad-partition)))

    (is (equal messages
               (loop
                  for message = (kf:poll consumer 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer))))))
