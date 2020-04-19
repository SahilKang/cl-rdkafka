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

(defpackage #:test/high-level/produce->consume
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/produce->consume)

(defun produce-messages (topic)
  (let ((producer (make-instance
                   'kf:producer
                   :conf (list "bootstrap.servers" *bootstrap-servers*)
                   :serde (lambda (x)
                            (babel:string-to-octets x :encoding :utf-8))))
        (messages '(("key-1" "Hello") ("key-2" "World") ("key-3" "!"))))
    (loop
       for (k v) in messages
       do (kf:send producer topic v :key k))

    (kf:flush producer)
    messages))

(defun consume-messages (topic)
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (list "bootstrap.servers" *bootstrap-servers*
                               "group.id" "consume-messages-group-id"
                               "enable.auto.commit" "false"
                               "auto.offset.reset" "earliest"
                               "offset.store.method" "broker"
                               "enable.partition.eof" "false")
                   :serde (lambda (x)
                            (babel:octets-to-string x :encoding :utf-8)))))
    (kf:subscribe consumer (list topic))

    (loop
       for message = (kf:poll consumer 5000)
       while message

       for key = (kf:key message)
       for value = (kf:value message)

       collect (list key value)

       do (kf:commit consumer))))

(test produce->consume
  (with-topics ((topic "test-produce-to-consume"))
    (let ((expected (produce-messages topic))
          (actual (consume-messages topic)))
      (is (equal expected actual)))))


(test deadlock
  (with-topics ((topic "deadlock-topic"))
    (let* ((expected-messages 100000)
           (actual-messages 0)
           (lock (bt:make-lock "deadlock"))
           (kernel (lparallel:make-kernel 5 :name "deadlock"))
           (channel (let ((lparallel:*kernel* kernel))
                      (lparallel:make-channel)))
           (producer (make-instance
                      'kf:producer
                      :conf (list "bootstrap.servers" *bootstrap-servers*)
                      :serde #'babel:string-to-octets))
           (consumer (make-instance
                      'kf:consumer
                      :conf (list "bootstrap.servers" *bootstrap-servers*
                                  "group.id" "deadlock-group-id"
                                  "enable.auto.commit" "false"
                                  "auto.offset.reset" "earliest"
                                  "offset.store.method" "broker"
                                  "enable.partition.eof" "false")
                      :serde #'babel:octets-to-string)))
      (labels ((process ()
                 (bt:with-lock-held (lock)
                   (incf actual-messages)
                   (kf:commit consumer :asyncp t)))
               (poll ()
                 (let (message)
                   (bt:with-lock-held (lock)
                     (setf message (kf:poll consumer 5)))
                   (when message
                     (lparallel:submit-task channel #'process))))
               (poll-loop ()
                 (loop
                    initially (kf:subscribe consumer topic)
                    repeat expected-messages
                    do (kf:send producer topic "deadlock-message")
                    finally (kf:flush producer))
                 (loop
                    repeat (* 2 expected-messages)
                    until (= expected-messages actual-messages)
                    do (poll))))
        (unwind-protect
             ;; pass or fail within 1 minute
             (loop
                initially (lparallel:submit-task channel #'poll-loop)
                repeat 30
                until (= expected-messages actual-messages)
                do (sleep 2)
                finally (is (= expected-messages actual-messages)))
          (let ((lparallel:*kernel* kernel))
            (lparallel:end-kernel)))))))
