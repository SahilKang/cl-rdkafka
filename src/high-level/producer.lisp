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

(in-package #:cl-rdkafka)

(defclass producer ()
  ((rd-kafka-producer
    :documentation "Pointer to rd_kafka_t struct.")
   (topic-name->handle
    :initform (make-hash-table :test #'equal)
    :documentation "Hash-table mapping topic-names to rd-kafka-topic handles.")
   (key-serde
    :initform nil
    :documentation "Function to map object to byte vector, or nil for identity.")
   (value-serde
    :initform nil
    :documentation "Function to map object to byte vector, or nil for identity."))
  (:documentation
   "A client that produces messages to kafka topics.

Example:

(ql:quickload :cl-rdkafka)

(let* ((serde (lambda (x) (babel:string-to-octets x :encoding :utf-8)))
       (messages '((\"key-1\" \"value-1\") (\"key-2\" \"value-2\")))
       (producer (make-instance 'kf:producer
                                :conf (kf:conf
                                       \"bootstrap.servers\" \"127.0.0.1:9092\")
                                :serde serde)))
  (loop
     for (k v) in messages
     do (kf:produce producer \"topic-name\" v :key k))

  (kf:flush producer (* 2 1000)))"))

(defgeneric produce (producer topic value &key key partition)
  (:documentation
   "Asynchronously produce a message to a kafka topic.

If partition is not specified, one is chosen using the topic's partitioner
function."))

(defgeneric flush (producer timeout-ms)
  (:documentation
   "Block for up to timeout-ms milliseconds while in-flight messages are
sent to kafka cluster."))

(defmethod initialize-instance :after
    ((producer producer) &key conf serde key-serde value-serde)
  (with-slots (rd-kafka-producer
               topic-name->handle
               (ks key-serde)
               (vs value-serde))
      producer
    (cffi:with-foreign-object (errstr :char +errstr-len+)
      (setf rd-kafka-producer (cl-rdkafka/ll:rd-kafka-new
                               cl-rdkafka/ll:rd-kafka-producer
                               (make-conf conf)
                               errstr
                               +errstr-len+))
      (when (cffi:null-pointer-p rd-kafka-producer)
        (error "~&Failed to allocate new producer: ~A"
               (cffi:foreign-string-to-lisp errstr :max-chars +errstr-len+))))
    (setf ks (or key-serde serde)
          vs (or value-serde serde))
    (tg:finalize
     producer
     (lambda ()
       (cl-rdkafka/ll:rd-kafka-flush rd-kafka-producer (* 5 1000))
       (loop
          for v being the hash-values of topic-name->handle
          do (cl-rdkafka/ll:rd-kafka-topic-destroy v))
       (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)))))

(defgeneric get-topic-handle (producer topic-name))

(defun make-topic (rd-kafka-producer topic-name)
  (let ((handle (cl-rdkafka/ll:rd-kafka-topic-new
                 rd-kafka-producer
                 topic-name
                 (cffi:null-pointer))))
    (when (cffi:null-pointer-p handle)
      (error "~&Failed to allocate topic object: ~A"
             (cl-rdkafka/ll:rd-kafka-err2str
              (cl-rdkafka/ll:rd-kafka-last-error))))
    handle))

(defmethod get-topic-handle ((producer producer) (topic-name string))
  (with-slots (topic-name->handle rd-kafka-producer) producer
    (let ((handle (gethash topic-name topic-name->handle)))
      (unless handle
        (setf handle (make-topic rd-kafka-producer topic-name)
              (gethash topic-name topic-name->handle) handle))
      handle)))

(defun ->bytes (object serde)
  (if (functionp serde)
      (funcall serde object)
      object))

(defun %produce (topic-handle partition key-bytes value-bytes)
  (let ((key-pointer (cffi:null-pointer))
        (value-pointer (cffi:null-pointer))
        (msg-flags cl-rdkafka/ll:rd-kafka-msg-f-free)
        (ret-val 0))
    (unwind-protect
         (setf key-pointer (bytes->pointer key-bytes)
               value-pointer (bytes->pointer value-bytes)
               ret-val (cl-rdkafka/ll:rd-kafka-produce
                        topic-handle
                        partition
                        msg-flags
                        value-pointer
                        (length value-bytes)
                        key-pointer
                        (length key-bytes)
                        (cffi:null-pointer)))
      (cffi:foreign-free key-pointer)
      (when (= -1 ret-val)
        (cffi:foreign-free value-pointer)))))

(defmethod produce ((producer producer) (topic string) value
                    &key (key nil key-p) partition)
  (with-slots (rd-kafka-producer key-serde value-serde) producer
    (let* ((topic-handle (get-topic-handle producer topic))
           (key-bytes (if key-p (->bytes key key-serde) (vector)))
           (value-bytes (->bytes value value-serde))
           (partition (if partition
                          partition
                          cl-rdkafka/ll:rd-kafka-partition-ua))
           (ret-val (%produce
                     topic-handle
                     partition
                     key-bytes
                     value-bytes)))
      (cl-rdkafka/ll:rd-kafka-poll rd-kafka-producer 0)
      (when (= -1 ret-val)
        (error "~&Failed to produce message to topic ~A: ~A"
               topic
               (cl-rdkafka/ll:rd-kafka-err2str
                (cl-rdkafka/ll:rd-kafka-last-error)))))))

(defmethod flush ((producer producer) (timeout-ms integer))
  (with-slots (rd-kafka-producer) producer
    (cl-rdkafka/ll:rd-kafka-flush rd-kafka-producer timeout-ms)))
