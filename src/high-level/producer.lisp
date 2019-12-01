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
    :type serializer
    :documentation "Serializer to map object to byte sequence.")
   (value-serde
    :type serializer
    :documentation "Serializer to map object to byte sequence.")
   (promises
    :initform (make-queue)
    :type queue
    :documentation
    "A queue of (promise key value) lists to fulfill after produce."))
  (:documentation
   "A client that produces messages to kafka topics.

Example:

(let ((producer (make-instance
                 'kf:producer
                 :conf '(\"bootstrap.servers\" \"127.0.0.1:9092\")
                 :serde (lambda (string)
                          (babel:string-to-octets string :encoding :utf-8))))
      (messages '((\"key-1\" \"value-1\")
                  (\"key-2\" \"value-2\"))))
  (loop
     for (k v) in messages
     do (kf:produce producer \"topic-name\" v :key k))

  (kf:flush producer 2000))"))

(defgeneric produce (producer topic value &key key partition headers)
  (:documentation
   "Asynchronously produce a message to a kafka topic and return a promise.

If PARTITION is not specified, one is chosen using the topic's
partitioner function.

HEADERS should be an alist of (string . byte-vector) pairs.

The returned blackbird:promise is either resolved with a MESSAGE or
rejected with a condition."))

(defgeneric flush (producer timeout-ms)
  (:documentation
   "Block for up to timeout-ms milliseconds while in-flight messages are
sent to kafka cluster."))

(cffi:defcallback message-delivery-callback :void
    ((rk :pointer)
     (rk-message :pointer)
     (opaque :pointer))
  (declare (ignore rk opaque)
           (special *promises*))
  (destructuring-bind (promise key value) (dequeue *promises*)
    (handler-case
        (blackbird-base:finish
         promise
         (rd-kafka-message->message rk-message
                                    (lambda (bytes)
                                      (declare (ignore bytes))
                                      key)
                                    (lambda (bytes)
                                      (declare (ignore bytes))
                                      value)))
      (condition (c)
        (blackbird-base:signal-error promise c)))))

(defmethod initialize-instance :after
    ((producer producer) &key conf (serde #'identity) key-serde value-serde)
  (with-slots (rd-kafka-producer
               (ks key-serde)
               (vs value-serde)
               promises)
      producer
    (with-conf rd-kafka-conf conf
      (cl-rdkafka/ll:rd-kafka-conf-set-dr-msg-cb
       rd-kafka-conf
       (cffi:callback message-delivery-callback))
      (cffi:with-foreign-object (errstr :char +errstr-len+)
        (setf rd-kafka-producer (cl-rdkafka/ll:rd-kafka-new
                                 cl-rdkafka/ll:rd-kafka-producer
                                 rd-kafka-conf
                                 errstr
                                 +errstr-len+))
        (when (cffi:null-pointer-p rd-kafka-producer)
          (error 'allocation-error
                 :name "producer"
                 :description (cffi:foreign-string-to-lisp
                               errstr :max-chars +errstr-len+)))))
    (setf ks (make-instance 'serializer
                            :name "key-serde"
                            :function (or key-serde serde))
          vs (make-instance 'serializer
                            :name "value-serde"
                            :function (or value-serde serde)))
    (tg:finalize
     producer
     (let ((*promises* promises))
       (declare (special *promises*))
       (lambda ()
         (cl-rdkafka/ll:rd-kafka-flush rd-kafka-producer 5000)
         (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer))))))

(defun add-header (headers name value)
  (let ((value-pointer (bytes->pointer value)))
    (unwind-protect
         (let ((err (cl-rdkafka/ll:rd-kafka-header-add
                     headers
                     name
                     (length name)
                     value-pointer
                     (length value))))
           ;; this should never return an error...however, those are
           ;; famous last words, so let's check the return value
           ;; anyway like the good engineers that we are
           (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error 'kafka-error
                    :description
                    (format nil "Failed to set header value for `~A`: `~A`"
                            name
                            (cl-rdkafka/ll:rd-kafka-err2str err)))))
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
             (error 'topic+partition-error
                    :description (cl-rdkafka/ll:rd-kafka-err2str err)
                    :topic topic
                    :partition partition)))
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
  (with-slots (rd-kafka-producer key-serde value-serde promises) producer
    (let ((key-bytes (if key-p (apply-serde key-serde key) (vector)))
          (value-bytes (apply-serde value-serde value))
          (partition (or partition cl-rdkafka/ll:rd-kafka-partition-ua)))
      (unwind-protect
           (progn
             (%produce rd-kafka-producer
                       topic
                       partition
                       key-bytes
                       value-bytes
                       headers)
             (let ((promise (blackbird-base:make-promise)))
               (enqueue promises (list promise key value))
               promise))
        (let ((*promises* promises))
          (declare (special *promises*))
          (cl-rdkafka/ll:rd-kafka-poll rd-kafka-producer 0))))))

(defmethod flush ((producer producer) (timeout-ms integer))
  (with-slots (rd-kafka-producer promises) producer
    (let ((*promises* promises))
      (declare (special *promises*))
      (let ((err (cl-rdkafka/ll:rd-kafka-flush rd-kafka-producer timeout-ms)))
        (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
          (if (eq err cl-rdkafka/ll:rd-kafka-resp-err--timed-out)
              (cerror "Ignore timeout and return from flush."
                      'kafka-error
                      :description "Flush timed out before finishing.")
              (error 'kafka-error
                     :description (cl-rdkafka/ll:rd-kafka-err2str err))))))))
