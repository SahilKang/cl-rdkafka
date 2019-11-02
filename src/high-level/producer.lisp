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

(defgeneric produce (producer topic value &key key partition headers)
  (:documentation
   "Asynchronously produce a message to a kafka topic.

If PARTITION is not specified, one is chosen using the topic's
partitioner function.

HEADERS should be an alist of (string . byte-vector) pairs."))

(defgeneric flush (producer timeout-ms)
  (:documentation
   "Block for up to timeout-ms milliseconds while in-flight messages are
sent to kafka cluster."))

(defmethod initialize-instance :after
    ((producer producer) &key conf serde key-serde value-serde)
  (with-slots (rd-kafka-producer
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
       (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)))))

(defun ->bytes (object serde)
  (if (functionp serde)
      (funcall serde object)
      object))

(defun add-header (headers name value)
  (let ((value-pointer (bytes->pointer value)))
    (unwind-protect
         (let ((err (cl-rdkafka/ll:rd-kafka-header-add
                     headers
                     name
                     (length name)
                     value-pointer
                     (length value))))
           (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error "~&Failed to set header value for ~S: ~S"
                    name
                    (cl-rdkafka/ll:rd-kafka-err2str err))))
      (cffi:foreign-free value-pointer))))

(defun make-headers (alist)
  (let ((headers (cl-rdkafka/ll:rd-kafka-headers-new (length alist))))
    (handler-case
        (loop
           for (name . value) in alist
           do (add-header headers name value)
           finally (return headers))
      (condition (c)
        (cl-rdkafka/ll:rd-kafka-headers-destroy headers)
        (error c)))))

(defun %produce
    (rd-kafka-producer topic partition key-bytes value-bytes headers)
  (let ((msg-flags cl-rdkafka/ll:rd-kafka-msg-f-free)
        err
        key-pointer
        value-pointer
        headers-pointer)
    (unwind-protect
         (progn
           (setf key-pointer (bytes->pointer key-bytes)
                 value-pointer (bytes->pointer value-bytes)
                 headers-pointer (make-headers headers)
                 err (cl-rdkafka/ll:rd-kafka-producev
                      rd-kafka-producer

                      :int cl-rdkafka/ll:rd-kafka-vtype-topic
                      :string topic

                      :int cl-rdkafka/ll:rd-kafka-vtype-value
                      :pointer value-pointer
                      cl-rdkafka/ll:size-t (length value-bytes)

                      :int cl-rdkafka/ll:rd-kafka-vtype-key
                      :pointer key-pointer
                      cl-rdkafka/ll:size-t (length key-bytes)

                      :int cl-rdkafka/ll:rd-kafka-vtype-partition
                      :int32 partition

                      :int cl-rdkafka/ll:rd-kafka-vtype-msgflags
                      :int msg-flags

                      :int cl-rdkafka/ll:rd-kafka-vtype-headers
                      :pointer headers-pointer

                      :int cl-rdkafka/ll:rd-kafka-vtype-end))
           (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error "~&Failed to produce message to topic: ~S: ~S"
                    topic
                    (cl-rdkafka/ll:rd-kafka-err2str err))))
      (when key-pointer
        (cffi:foreign-free key-pointer))
      (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (when value-pointer
          (cffi:foreign-free value-pointer))
        (when headers-pointer
          (cl-rdkafka/ll:rd-kafka-headers-destroy headers-pointer))))))

(defmethod produce
    ((producer producer)
     (topic string)
     value
     &key (key nil key-p) partition headers)
  (with-slots (rd-kafka-producer key-serde value-serde) producer
    (let ((key-bytes (if key-p (->bytes key key-serde) (vector)))
          (value-bytes (->bytes value value-serde))
          (partition (or partition cl-rdkafka/ll:rd-kafka-partition-ua)))
      (unwind-protect
           (%produce rd-kafka-producer
                     topic
                     partition
                     key-bytes
                     value-bytes
                     headers)
        (cl-rdkafka/ll:rd-kafka-poll rd-kafka-producer 0)))))

(defmethod flush ((producer producer) (timeout-ms integer))
  (with-slots (rd-kafka-producer) producer
    (cl-rdkafka/ll:rd-kafka-flush rd-kafka-producer timeout-ms)))
