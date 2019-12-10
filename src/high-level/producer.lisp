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
   (rd-kafka-queue
    :documentation "Pointer to rd_kafka_queue_t struct.")
   (key-serde
    :type serializer
    :documentation "Serializer to map object to byte sequence.")
   (value-serde
    :type serializer
    :documentation "Serializer to map object to byte sequence."))
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

  (kf:flush producer))"))

(defgeneric produce (producer topic value &key key partition headers)
  (:documentation
   "Asynchronously produce a message to a kafka topic and return a promise.

If PARTITION is not specified, one is chosen using the topic's
partitioner function.

HEADERS should be an alist of (string . byte-vector) pairs.

The returned blackbird:promise is either resolved with a MESSAGE or
rejected with a condition."))

(defgeneric flush (producer)
  (:documentation
   "Block while in-flight messages are sent to kafka cluster."))

(defvar +poll-table-lock+ (bt:make-lock "poll-table-lock"))

(defvar +poll-table+ (make-hash-table)
  "Maps an rd_kafka_queue_t pointer address to a queue of (promise key value) lists.")

(defvar +background-thread+ nil
  "Background thread to poll all producers.")

;; +background-thread+ will poll all producers to keep the
;; message-delivery-callback fed. I tried using
;; rd_kafka_interceptor_add_on_acknowledgement, but was getting a
;; sporadic segfault from the callback (so defcallback might be
;; failing me). I don't want users to concern themselves with polling
;; the producers so this background thread at least gives me the api I
;; want to expose; however, I should try and get librdkafka to poll
;; with one of its background threads instead.

(defun assert-produce-event (rd-kafka-event)
  (let ((event-type (cl-rdkafka/ll:rd-kafka-event-type rd-kafka-event))
        (err (cl-rdkafka/ll:rd-kafka-event-error rd-kafka-event)))
    (unless (= event-type cl-rdkafka/ll:rd-kafka-event-dr)
      (error "Expected event-type `~A`, not `~A`"
             cl-rdkafka/ll:rd-kafka-event-dr
             event-type))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error 'kafka-error
             :description (cl-rdkafka/ll:rd-kafka-err2str err)))))

(defun process-produce-event (rd-kafka-event queue)
  (assert-produce-event rd-kafka-event)
  (loop
     for rd-kafka-message = (cl-rdkafka/ll:rd-kafka-event-message-next
                             rd-kafka-event)
     until (cffi:null-pointer-p rd-kafka-message)

     for (promise key value) = (dequeue queue)
     for key-fn = (lambda (bytes)
                    (declare (ignore bytes))
                    key)
     for value-fn = (lambda (bytes)
                      (declare (ignore bytes))
                      value)
     do (handler-case
            (blackbird-base:finish
             promise
             (rd-kafka-message->message rd-kafka-message key-fn value-fn))
          (condition (c)
            (blackbird-base:signal-error promise c)))))

(defun process-produces (rd-kafka-queue queue)
  (loop
     for event = (cl-rdkafka/ll:rd-kafka-queue-poll rd-kafka-queue 0)
     until (cffi:null-pointer-p event)
     do (unwind-protect
             (process-produce-event event queue)
          (cl-rdkafka/ll:rd-kafka-event-destroy event))))

(defun poll-all-produce-queues ()
  (maphash (lambda (address queue)
             (let ((rd-kafka-queue (cffi:make-pointer address)))
               (handler-case
                   (process-produces rd-kafka-queue queue)
                 (condition ()))))
           +poll-table+))

(defun poll-loop ()
  (loop
     do
       (handler-case
           (bt:with-lock-held (+poll-table-lock+)
             (if (zerop (hash-table-count +poll-table+))
                 (return-from poll-loop)
                 (poll-all-produce-queues)))
         (condition ()))
       (sleep 0.5)))

(defun make-produce-promise (rd-kafka-queue key value)
  (let ((promise (blackbird-base:make-promise))
        (address (cffi:pointer-address rd-kafka-queue)))
    (bt:with-lock-held (+poll-table-lock+)
      (let ((queue (gethash address +poll-table+)))
        (enqueue queue (list promise key value))))
    promise))

(defun add-queue-to-poll-table (rd-kafka-queue)
  (let ((address (cffi:pointer-address rd-kafka-queue)))
    (bt:with-lock-held (+poll-table-lock+)
      (handler-case
          (progn
            (setf (gethash address +poll-table+) (make-queue))
            (unless (and +background-thread+
                         (bt:thread-alive-p +background-thread+))
              (setf +background-thread+
                    (bt:make-thread #'poll-loop :name "poll-loop"))))
        (condition (c)
          (remhash address +poll-table+)
          (error c))))))

(defun remove-queue-from-poll-table (rd-kafka-queue)
  (let ((address (cffi:pointer-address rd-kafka-queue)))
    (bt:with-lock-held (+poll-table-lock+)
      (remhash address +poll-table+))))

(defun get-last-promise (rd-kafka-queue)
  (bt:with-lock-held (+poll-table-lock+)
    (loop
       with address = (cffi:pointer-address rd-kafka-queue)
       with queue = (gethash address +poll-table+)
       with head = (queue-head queue)

       repeat (1- (queue-length queue))
       do (setf head (cdr head))

       finally (return (first (car head))))))

(defun make-producer-finalizer (rd-kafka-producer rd-kafka-queue)
  (lambda ()
    (remove-queue-from-poll-table rd-kafka-queue)
    (cl-rdkafka/ll:rd-kafka-queue-destroy rd-kafka-queue)
    (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)))

(defmethod initialize-instance :after
    ((producer producer) &key conf (serde #'identity) key-serde value-serde)
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
        (add-queue-to-poll-table rd-kafka-queue)
      (condition (c)
        (cl-rdkafka/ll:rd-kafka-queue-destroy rd-kafka-queue)
        (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)
        (error c)))
    (setf ks (make-instance 'serializer
                            :name "key-serde"
                            :function (or key-serde serde))
          vs (make-instance 'serializer
                            :name "value-serde"
                            :function (or value-serde serde)))
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
  (with-slots (rd-kafka-producer rd-kafka-queue key-serde value-serde) producer
    (let ((key-bytes (if key-p (apply-serde key-serde key) (vector)))
          (value-bytes (apply-serde value-serde value))
          (partition (or partition cl-rdkafka/ll:rd-kafka-partition-ua)))
      (%produce rd-kafka-producer
                topic
                partition
                key-bytes
                value-bytes
                headers)
      (make-produce-promise rd-kafka-queue key value))))

;; using rd_kafka_flush with rd_kafka_event_dr would cause a sporadic
;; NULL dereference for some reason. My gut feeling is that some race
;; condition was occurring because the problem would go away when
;; either stepping through with the debugger or sleeping before the
;; rd_kafka_flush call. In either case, it's easy enough to implement
;; flush ourselves:
(defmethod flush ((producer producer))
  (with-slots (rd-kafka-queue) producer
    (handler-case
        ;; this ignores any rejected promises...not sure if that's
        ;; good or bad
        (force-promise (get-last-promise rd-kafka-queue))
      (condition ()))))
