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

(in-package #:test/low-level/producer)

(defparameter *messages* (make-array 0
                                     :element-type 'string
                                     :adjustable t
                                     :fill-pointer 0))

(defcallback message-delivery-report-callback :void
    ((rk :pointer)
     (rk-message :pointer)
     (opaque :pointer))
  (let* ((*rk-message (mem-ref rk-message '(:struct rd-kafka-message)))
         (err (getf *rk-message 'cl-rdkafka/ll:err)))
    (if (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (let* ((payload (getf *rk-message 'cl-rdkafka/ll:payload))
               (len (getf *rk-message 'cl-rdkafka/ll:len))
               (message (foreign-string-to-lisp payload :max-chars (- len 1))))
          (vector-push-extend message *messages*))
        (error (format nil
                       "Message delivery failed: ~A~%"
                       (rd-kafka-err2str err))))))

(defun make-conf (brokers errstr errstr-len)
  (let ((conf (rd-kafka-conf-new)))
    (if (eq 'cl-rdkafka/ll:rd-kafka-conf-ok
            (rd-kafka-conf-set conf
                               "bootstrap.servers"
                               brokers
                               errstr
                               errstr-len))
        (progn
          (rd-kafka-conf-set-dr-msg-cb
           conf
           (callback message-delivery-report-callback))
          conf)
        (error (format nil
                       "make-conf failed with: ~A~%"
                       (foreign-string-to-lisp
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
  (let ((topic (rd-kafka-topic-new producer topic-name (null-pointer))))
    (unless topic
      (rd-kafka-destroy producer)
      (error (format nil
                     "Failed to create topic object: ~A~%"
                     (rd-kafka-err2str (rd-kafka-last-error)))))
    topic))

(defun init (brokers topic-name)
  (let (producer topic conf (errstr-len 512))
    (with-foreign-object (errstr :char errstr-len)
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
                    (null-pointer)
                    0
                    (null-pointer)))

(defun produce (producer topic message)
  (let ((len (+ 1 (length message))))
    (with-foreign-pointer-as-string (buf len)
      (lisp-string-to-foreign message buf len)
      (when (= -1 (produce-buf topic buf len))
        (error (format nil
                       "Failed to produce message ~A to topic ~A: ~A~%"
                       message
                       (rd-kafka-topic-name topic)
                       (rd-kafka-err2str (rd-kafka-last-error)))))))
  (rd-kafka-poll producer 0))

(def-test producer ()
  (let ((topic-name "producer-test-topic")
        (expected '("Hello" "World" "!")))
    (destructuring-bind (producer topic) (init "kafka:9092" topic-name)
      (produce producer topic "Hello")
      (produce producer topic "World")
      (produce producer topic "!")

      (rd-kafka-flush producer (* 10 1000))
      (rd-kafka-topic-destroy topic)
      (rd-kafka-destroy producer))
    (is (and (= (length expected) (length *messages*))
             (every #'string= expected *messages*)))))
