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

(defpackage #:test/low-level/consumer
  (:use #:cl #:1am))

(in-package #:test/low-level/consumer)

(defparameter *messages* (make-array 0
                                     :element-type 'string
                                     :adjustable t
                                     :fill-pointer 0))

(defun consume-message (rk-message)
  (let* ((*rk-message (cffi:mem-ref
                       rk-message
                       '(:struct cl-rdkafka/ll:rd-kafka-message)))
         (err (getf *rk-message 'cl-rdkafka/ll:err))
         (len (getf *rk-message 'cl-rdkafka/ll:len))
         (payload (getf *rk-message 'cl-rdkafka/ll:payload)))
    (when (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (let ((message (cffi:foreign-string-to-lisp payload :max-chars len)))
        (vector-push-extend message *messages*)))))

(defun make-conf (group-id errstr errstr-len)
  (let ((conf (cl-rdkafka/ll:rd-kafka-conf-new)))
    (cl-rdkafka/ll:rd-kafka-conf-set
     conf "group.id" group-id errstr errstr-len)
    (cl-rdkafka/ll:rd-kafka-conf-set
     conf "enable.partition.eof" "true" (cffi:null-pointer) 0)
    conf))

(defun make-topic-conf (conf errstr errstr-len)
  (let ((topic-conf (cl-rdkafka/ll:rd-kafka-topic-conf-new)))
    (cl-rdkafka/ll:rd-kafka-topic-conf-set
     topic-conf "offset.store.method" "broker" errstr errstr-len)
    (cl-rdkafka/ll:rd-kafka-topic-conf-set
     topic-conf "auto.offset.reset" "earliest" errstr errstr-len)
    (cl-rdkafka/ll:rd-kafka-conf-set-default-topic-conf conf topic-conf)))

(defun make-topic+partition-list (topics)
  (let ((topic+partitions (cl-rdkafka/ll:rd-kafka-topic-partition-list-new
                           (length topics))))
    (loop
       for topic in topics
       do (cl-rdkafka/ll:rd-kafka-topic-partition-list-add
           topic+partitions topic -1))
    topic+partitions))

(defun make-consumer (conf brokers topics errstr errstr-len)
  (let* ((consumer (cl-rdkafka/ll:rd-kafka-new
                    cl-rdkafka/ll:rd-kafka-consumer
                    conf
                    errstr
                    errstr-len))
         (topic+partitions (make-topic+partition-list topics)))
    (unless consumer
      (error (format nil "Failed to create new consumer: ~A~%" errstr)))
    (cl-rdkafka/ll:rd-kafka-brokers-add consumer brokers)
    (cl-rdkafka/ll:rd-kafka-poll-set-consumer consumer)
    (cl-rdkafka/ll:rd-kafka-subscribe consumer topic+partitions)
    (list consumer topic+partitions)))

(defun consume (consumer)
  (let ((message (cl-rdkafka/ll:rd-kafka-consumer-poll consumer 5000)))
    (unless (cffi:null-pointer-p message)
      (consume-message message)
      (cl-rdkafka/ll:rd-kafka-message-destroy message))))

(defun init (group-id brokers topics)
  (let (conf topic-conf consumer&topic+partitions (errstr-len 512))
    (cffi:with-foreign-object (errstr :char errstr-len)
      (setf conf (make-conf group-id errstr errstr-len)
            topic-conf (make-topic-conf conf errstr errstr-len)

            consumer&topic+partitions
            (make-consumer conf brokers topics errstr errstr-len)))
    consumer&topic+partitions))

(defun destroy (consumer topic+partitions)
  (cl-rdkafka/ll:rd-kafka-consumer-close consumer)
  (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy topic+partitions)
  (cl-rdkafka/ll:rd-kafka-destroy consumer))

(test consumer
  (let ((bootstrap-servers "kafka:9092")
        (topic "consumer-test-topic")
        (expected '("Hello" "World" "!")))
    (uiop:run-program
     (format nil "echo -n '~A' | kafkacat -P -D '|' -b '~A' -t '~A'"
             (reduce (lambda (agg s) (format nil "~A|~A" agg s)) expected)
             bootstrap-servers
             topic)
     :force-shell t
     :output nil
     :error-output nil)
    (sleep 2)

    (destructuring-bind
          (consumer topic+partitions)
        (init "consumer-group-id" bootstrap-servers (list topic))
      (loop repeat (length expected) do (consume consumer))
      (destroy consumer topic+partitions))
    (is (equal expected (coerce *messages* 'list)))))
