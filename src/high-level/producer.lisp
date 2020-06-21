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

(in-package #:cl-rdkafka)

(defclass producer ()
  ((rd-kafka-producer
    :documentation "Pointer to rd_kafka_t struct.")
   (rd-kafka-queue
    :documentation "Pointer to rd_kafka_queue_t struct.")
   (last-promise
    :initform nil
    :documentation "Latest promise from send call.")
   (key-serde
    :type serializer
    :documentation "SERIALIZER to map object to byte sequence.")
   (value-serde
    :type serializer
    :documentation "SERIALIZER to map object to byte sequence."))
  (:documentation
   "A client that produces messages to kafka topics.

MAKE-INSTANCE accepts the following keyword args:

  * CONF: A required plist, alist, or hash-table mapping config keys
          to their respective values; both keys and values should be
          strings. The provided key-value pairs are passed as-is to
          librdkafka, so consult the librdkafka config docs for more
          info.

  * SERDE: An optional unary function accepting an object and
           returning a byte sequence; defaults to #'identity.

  * KEY-SERDE: An optional unary function used to serialize message
               keys; defaults to SERDE.

  * VALUE-SERDE: An optional unary function used to serialize message
                 values; defaults to SERDE.

Example:

(let ((producer (make-instance
                 'kf:producer
                 :conf '(\"bootstrap.servers\" \"127.0.0.1:9092\"
                         \"enable.idempotence\" \"true\")
                 :serde #'babel:string-to-octets))
      (messages '((\"key-1\" \"value-1\")
                  (\"key-2\" \"value-2\"))))
  (loop
     for (k v) in messages
     do (kf:send producer \"topic-name\" v :key k))

  (kf:flush producer))"))

(defgeneric send (producer topic value &key key partition headers timestamp))

(defgeneric flush (producer))

(defun process-send-event (rd-kafka-event queue)
  (assert-expected-event rd-kafka-event cl-rdkafka/ll:rd-kafka-event-dr)
  (let ((err (cl-rdkafka/ll:rd-kafka-event-error rd-kafka-event)))
    (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (let ((promise (first (lparallel.queue:pop-queue queue))))
        (lparallel:fulfill promise (make-rdkafka-error err)))
      (return-from process-send-event)))
  (loop
     for message = (cl-rdkafka/ll:rd-kafka-event-message-next rd-kafka-event)
     until (cffi:null-pointer-p message)

     for (promise key value) = (lparallel.queue:pop-queue queue)
     for key-fn = (lambda (bytes)
                    (declare (ignore bytes))
                    key)
     for value-fn = (lambda (bytes)
                      (declare (ignore bytes))
                      value)
     do (handler-case
            (lparallel:fulfill promise
              (rd-kafka-message->message message key-fn value-fn))
          (condition (c)
            (lparallel:fulfill promise c)))))

(defun make-producer-finalizer (rd-kafka-producer rd-kafka-queue)
  (lambda ()
    (deregister-rd-kafka-queue rd-kafka-queue)
    (cl-rdkafka/ll:rd-kafka-queue-destroy rd-kafka-queue)
    (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)))

(defmethod initialize-instance :after
    ((producer producer)
     &key conf (serde #'identity) (key-serde serde) (value-serde serde))
  (with-slots (rd-kafka-producer
               rd-kafka-queue
               (ks key-serde)
               (vs value-serde))
      producer
    (with-conf rd-kafka-conf conf
      (cl-rdkafka/ll:rd-kafka-conf-set-events
       rd-kafka-conf
       cl-rdkafka/ll:rd-kafka-event-dr)
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
    (setf rd-kafka-queue (cl-rdkafka/ll:rd-kafka-queue-get-main rd-kafka-producer))
    (when (cffi:null-pointer-p rd-kafka-queue)
      (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)
      (error 'allocation-error :name "queue"))
    (handler-case
        (register-rd-kafka-queue rd-kafka-queue #'process-send-event)
      (condition (c)
        (cl-rdkafka/ll:rd-kafka-queue-destroy rd-kafka-queue)
        (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)
        (error c)))
    (setf ks (make-instance 'serializer
                            :name "key-serde"
                            :function key-serde)
          vs (make-instance 'serializer
                            :name "value-serde"
                            :function value-serde))
    (tg:finalize producer (make-producer-finalizer rd-kafka-producer rd-kafka-queue))))

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
           (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error (make-rdkafka-error err))))
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

(defun %send
    (rd-kafka-producer topic partition key-bytes value-bytes headers timestamp)
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

                      :int cl-rdkafka/ll:rd-kafka-vtype-timestamp
                      :int64 timestamp

                      :int cl-rdkafka/ll:rd-kafka-vtype-msgflags
                      :int msg-flags

                      :int cl-rdkafka/ll:rd-kafka-vtype-headers
                      :pointer headers-pointer

                      :int cl-rdkafka/ll:rd-kafka-vtype-end))
           (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error (make-partition-error err topic partition))))
      (when key-pointer
        (cffi:foreign-free key-pointer))
      (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (when value-pointer
          (cffi:foreign-free value-pointer))
        (when headers-pointer
          (cl-rdkafka/ll:rd-kafka-headers-destroy headers-pointer))))))

(defmethod send
    ((producer producer)
     (topic string)
     value
     &key (key nil key-p) partition headers timestamp)
  "Asynchronously send a message and return a MESSAGE FUTURE.

If PARTITION is not specified, one is chosen using the TOPIC's
partitioner function.

If specified, HEADERS should be an alist mapping strings to
byte-vectors.

TIMESTAMP is the number of milliseconds since the UTC epoch. If not
specified, one will be generated by this call.

May signal PARTITION-ERROR or condition from PRODUCER's serde. A
STORE-FUNCTION restart will be provided if it's a serde condition."
  (with-slots (rd-kafka-producer
               rd-kafka-queue
               key-serde
               value-serde
               last-promise)
      producer
    (let ((key-bytes (if key-p (apply-serde key-serde key) (vector)))
          (value-bytes (apply-serde value-serde value))
          (partition (or partition cl-rdkafka/ll:rd-kafka-partition-ua)))
      (bt:with-lock-held (+address->queue-lock+)
        (%send rd-kafka-producer
               topic
               partition
               key-bytes
               value-bytes
               headers
               (or timestamp 0))
        (let ((promise (lparallel:promise)))
          (enqueue-payload rd-kafka-queue (list promise key value))
          (setf last-promise promise)
          (make-instance 'future :promise promise :client producer))))))

;; using rd_kafka_flush with rd_kafka_event_dr would cause a sporadic
;; NULL dereference for some reason. My gut feeling is that some race
;; condition was occurring because the problem would go away when
;; either stepping through with the debugger or sleeping before the
;; rd_kafka_flush call. In either case, it's easy enough to implement
;; flush ourselves:
(defmethod flush ((producer producer))
  "Block while in-flight messages are sent to kafka cluster."
  (with-slots (last-promise) producer
    (lparallel:force last-promise))
  nil)
