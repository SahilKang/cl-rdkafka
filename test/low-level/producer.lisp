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

(defpackage #:test/low-level/producer
  (:use #:cl #:cl-rdkafka/low-level #:1am))

(in-package #:test/low-level/producer)

(defun make-conf (brokers errstr errstr-len)
  (let ((conf (rd-kafka-conf-new)))
    (if (eq 'cl-rdkafka/ll:rd-kafka-conf-ok
            (rd-kafka-conf-set conf
                               "bootstrap.servers"
                               brokers
                               errstr
                               errstr-len))
        conf
        (error (format nil
                       "make-conf failed with: ~A~%"
                       (cffi:foreign-string-to-lisp
                        errstr
                        :max-chars (- errstr-len 1)))))))

(defun make-producer (conf errstr errstr-len)
  (let ((producer (rd-kafka-new cl-rdkafka/ll:rd-kafka-producer
                                conf
                                errstr
                                errstr-len)))
    (unless producer
      (error (format nil
                     "Failed to create new producer: ~A~%"
                     errstr)))
    producer))

(defun make-topic (producer topic-name)
  (let ((topic (rd-kafka-topic-new producer topic-name (cffi:null-pointer))))
    (unless topic
      (rd-kafka-destroy producer)
      (error (format nil
                     "Failed to create topic object: ~A~%"
                     (rd-kafka-err2str (rd-kafka-last-error)))))
    topic))

(defun init (brokers topic-name)
  (let (producer topic conf (errstr-len 512))
    (cffi:with-foreign-object (errstr :char errstr-len)
      (setf conf (make-conf brokers errstr errstr-len)
            producer (make-producer conf errstr errstr-len)
            topic (make-topic producer topic-name)))
    (list producer topic)))

(defun produce-buf (topic buf len)
  (rd-kafka-produce topic
                    rd-kafka-partition-ua
                    rd-kafka-msg-f-copy
                    buf
                    len
                    (cffi:null-pointer)
                    0
                    (cffi:null-pointer)))

(defun produce (producer topic message)
  (cffi:with-foreign-string (buf message)
    (when (= -1 (produce-buf topic buf (length message)))
      (error (format nil
                     "Failed to produce message ~A to topic ~A: ~A~%"
                     message
                     (rd-kafka-topic-name topic)
                     (rd-kafka-err2str (rd-kafka-last-error))))))
  (rd-kafka-poll producer 0))

(test producer
  (let ((topic-name "producer-test-topic")
        (bootstrap-servers "kafka:9092")
        (expected '("Hello" "World" "!")))
    (destructuring-bind (producer topic) (init bootstrap-servers topic-name)
      (mapcar (lambda (message)
                (produce producer topic message))
              expected)
      (rd-kafka-flush producer 5000)
      (rd-kafka-topic-destroy topic)
      (rd-kafka-destroy producer))
    (is (equal expected (uiop:run-program
                         (format nil "kafkacat -Ce -b '~A' -t '~A'"
                                 bootstrap-servers
                                 topic-name)
                         :force-shell t
                         :output :lines
                         :error-output nil)))))
