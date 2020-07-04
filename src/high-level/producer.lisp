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

(defgeneric initialize-transactions (producer timeout-ms))

(defgeneric begin-transaction (producer))

(defgeneric commit-transaction (producer timeout-ms))

(defgeneric abort-transaction (producer timeout-ms))

(defgeneric send-offsets-to-transaction (producer consumer offsets timeout-ms)
  (:documentation
   "Send OFFSETS to CONSUMER group coordinator and mark them as part of the ongoing transaction.

A transaction must have been started by BEGIN-TRANSACTION.

This method will block for up to TIMEOUT-MS milliseconds.

OFFSETS should be associated with CONSUMER, and will be considered
committed only if the ongoing transaction is committed
successfully. Each offset should refer to the next message that the
CONSUMER POLL method should return: the last processed message's
offset + 1. Invalid offsets will be ignored.

CONSUMER should have enable.auto.commit set to false and should not
commit offsets itself through the COMMIT method.

This method should be called at the end of a
consume->transform->produce cycle, before calling COMMIT-TRANSACTION.

May signal:

  * RETRYABLE-OPERATION-ERROR, in which case a RETRY-OPERATION and
    ABORT restart will be provided.

  * ABORT-REQUIRED-ERROR, in which case an ABORT restart will be
    provided.

  * TRANSACTION-ERROR

  * FATAL-ERROR"))

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

(defmethod initialize-transactions ((producer producer) (timeout-ms integer))
  "Block for up to TIMEOUT-MS milliseconds and initialize transactions for PRODUCER.

When transactional.id is configured, this method needs to be called
exactly once before any other methods on PRODUCER.

This method:

  1) Ensures any transactions initiated by previous producer instances
     with the same transactional.id are completed:

       * If the previous instance had failed with an in-progress
         transaction, it will be aborted.

       * If the previous transaction had started committing, but had
         not yet finished, this method waits for it to finish.

  2) Acquires the internal producer id and epoch to use with all
     future transactional messages sent by PRODUCER. This will be used
     to fence out any previous instances.

May signal a FATAL-ERROR, TRANSACTION-ERROR, or
RETRYABLE-OPERATION-ERROR. A RETRY-OPERATION restart will be provided
if it's a RETRYABLE-OPERATION-ERROR."
  (with-slots (rd-kafka-producer) producer
    (let ((err (cl-rdkafka/ll:rd-kafka-init-transactions
                rd-kafka-producer
                timeout-ms)))
      (unless (cffi:null-pointer-p err)
        (unwind-protect
             (restart-case
                 (cond
                   ((cl-rdkafka/ll:rd-kafka-error-is-retriable err)
                    (error (error->condition 'retryable-operation-error err)))
                   ((cl-rdkafka/ll:rd-kafka-error-is-fatal err)
                    (error (error->condition 'fatal-error err)))
                   (t
                    (error (error->condition 'transaction-error err))))
               (retry-operation ()
                 :report "Retry initializing transactions."
                 :test (lambda (condition)
                         (typep condition 'retryable-operation-error))
                 (initialize-transactions producer timeout-ms)))
          (cl-rdkafka/ll:rd-kafka-error-destroy err))))))

(defmethod begin-transaction ((producer producer))
  "Begin a transaction.

INITIALIZE-TRANSACTIONS must have been called exactly once before this
method, and only one transaction can be in progress at a time for
PRODUCER.

The transaction can be committed with COMMIT-TRANSACTION or aborted
with ABORT-TRANSACTION.

May signal FATAL-ERROR or TRANSACTION-ERROR."
  (with-slots (rd-kafka-producer) producer
    (let ((err (cl-rdkafka/ll:rd-kafka-begin-transaction rd-kafka-producer)))
      (unless (cffi:null-pointer-p err)
        (unwind-protect
             (if (cl-rdkafka/ll:rd-kafka-error-is-fatal err)
                 (error (error->condition 'fatal-error err))
                 (error (error->condition 'transaction-error err)))
          (cl-rdkafka/ll:rd-kafka-error-destroy err))))))

(defmethod commit-transaction ((producer producer) (timeout-ms integer))
  "Block for up to TIMEOUT-MS milliseconds and commit the ongoing transaction.

A transaction must have been started by BEGIN-TRANSACTION.

This method will flush all enqueued messages before issuing the
commit. If any of the messages fails to be sent, an
ABORT-REQUIRED-ERROR will be signalled.

May signal:

  * RETRYABLE-OPERATION-ERROR, in which case a RETRY-OPERATION and
    ABORT restart will be provided.

  * ABORT-REQUIRED-ERROR, in which case an ABORT restart will be
    provided.

  * TRANSACTION-ERROR

  * FATAL-ERROR"
  (with-slots (rd-kafka-producer) producer
    (let ((err (cl-rdkafka/ll:rd-kafka-commit-transaction
                rd-kafka-producer
                timeout-ms)))
      (unless (cffi:null-pointer-p err)
        (unwind-protect
             (restart-case
                 (cond
                   ((cl-rdkafka/ll:rd-kafka-error-is-retriable err)
                    (error (error->condition 'retryable-operation-error err)))
                   ((cl-rdkafka/ll:rd-kafka-error-txn-requires-abort err)
                    (error (error->condition 'abort-required-error err)))
                   ((cl-rdkafka/ll:rd-kafka-error-is-fatal err)
                    (error (error->condition 'fatal-error err)))
                   (t
                    (error (error->condition 'transaction-error err))))
               (retry-operation ()
                 :report "Retry committing transaction."
                 :test (lambda (condition)
                         (typep condition 'retryable-operation-error))
                 (commit-transaction producer timeout-ms))
               (abort ()
                 :report "Abort transaction."
                 :test (lambda (condition)
                         (typep condition
                                '(or retryable-operation-error abort-required-error)))
                 (handler-case
                     (abort-transaction producer 0)
                   (retryable-operation-error () nil))))
          (cl-rdkafka/ll:rd-kafka-error-destroy err))))))

(defmethod abort-transaction ((producer producer) (timeout-ms integer))
  "Block for up to TIMEOUT-MS milliseconds and abort the ongoing transaction.

A transaction must have been started by BEGIN-TRANSACTION.

This method will purge all enqueued messages before issuing the
abort.

May signal a FATAL-ERROR, TRANSACTION-ERROR, or
RETRYABLE-OPERATION-ERROR. A RETRY-OPERATION and CONTINUE restart will
be provided if it's a RETRYABLE-OPERATION-ERROR."
  (with-slots (rd-kafka-producer) producer
    (let ((err (cl-rdkafka/ll:rd-kafka-abort-transaction
                rd-kafka-producer
                timeout-ms)))
      (unless (cffi:null-pointer-p err)
        (unwind-protect
             (restart-case
                 (cond
                   ((cl-rdkafka/ll:rd-kafka-error-is-retriable err)
                    (error (error->condition 'retryable-operation-error err)))
                   ((cl-rdkafka/ll:rd-kafka-error-is-fatal err)
                    (error (error->condition 'fatal-error err)))
                   (t
                    (error (error->condition 'transaction-error err))))
               (retry-operation ()
                 :report "Try aborting again."
                 :test (lambda (condition)
                         (typep condition 'retryable-operation-error))
                 (abort-transaction producer timeout-ms))
               (continue ()
                 :report "Ignore and continue."
                 :test (lambda (condition)
                         (typep condition 'retryable-operation-error))
                 nil))
          (cl-rdkafka/ll:rd-kafka-error-destroy err))))))

(defmacro with-consumer-group-metadata (metadata consumer &body body)
  (let ((rd-kafka-consumer (gensym)))
    `(with-slots ((,rd-kafka-consumer rd-kafka-consumer)) ,consumer
       (let ((,metadata (cl-rdkafka/ll:rd-kafka-consumer-group-metadata
                         ,rd-kafka-consumer)))
         (unwind-protect
              (progn
                ,@body)
           (unless (cffi:null-pointer-p ,metadata)
             (cl-rdkafka/ll:rd-kafka-consumer-group-metadata-destroy ,metadata)))))))

(defun %send-offsets-to-transaction (producer consumer offsets timeout-ms)
  (with-consumer-group-metadata metadata consumer
    (with-slots (rd-kafka-producer) producer
      (let ((err (cl-rdkafka/ll:rd-kafka-send-offsets-to-transaction
                  rd-kafka-producer
                  offsets
                  metadata
                  timeout-ms)))
        (unless (cffi:null-pointer-p err)
          (unwind-protect
               (restart-case
                   (cond
                     ((cl-rdkafka/ll:rd-kafka-error-is-retriable err)
                      (error (error->condition 'retryable-operation-error err)))
                     ((cl-rdkafka/ll:rd-kafka-error-txn-requires-abort err)
                      (error (error->condition 'abort-required-error err)))
                     ((cl-rdkafka/ll:rd-kafka-error-is-fatal err)
                      (error (error->condition 'fatal-error err)))
                     (t
                      (error (error->condition 'transaction-error err))))
                 (retry-operation ()
                   :report "Retry sending offsets to transaction."
                   :test (lambda (condition)
                           (typep condition 'retryable-operation-error))
                   (%send-offsets-to-transaction producer consumer offsets timeout-ms))
                 (abort ()
                   :report "Abort transaction."
                   :test (lambda (condition)
                           (typep condition
                                  '(or retryable-operation-error abort-required-error)))
                   (handler-case
                       (abort-transaction producer 0)
                     (retryable-operation-error () nil))))
            (cl-rdkafka/ll:rd-kafka-error-destroy err)))))))

(defmethod send-offsets-to-transaction
    ((producer producer)
     (consumer consumer)
     (offsets list)
     (timeout-ms integer))
  "OFFSETS should be an alist mapping (topic . partition) cons cells
to either (offset . metadata) cons cells or lone offset values."
  (etypecase (first offsets)
    ((or message null) (call-next-method))
    (cons
     (with-toppar-list toppar-list (alloc-toppar-list-from-alist offsets)
       (%send-offsets-to-transaction producer consumer toppar-list timeout-ms)))))

(defmethod send-offsets-to-transaction
    ((producer producer)
     (consumer consumer)
     (offsets hash-table)
     (timeout-ms integer))
  "OFFSETS should be a hash-table mapping (topic . partition) cons
cells to either (offset . metadata) cons cells or lone offset values."
  (let ((vector (make-array (hash-table-count offsets)
                            :adjustable nil
                            :fill-pointer 0)))
    (maphash (lambda (k v)
               (vector-push (cons k v) vector))
             offsets)
    (with-toppar-list toppar-list (alloc-toppar-list-from-alist vector)
      (%send-offsets-to-transaction producer consumer toppar-list timeout-ms))))

(defun messages->toppar-list (messages)
  (let* ((hash-table (reduce
                      (lambda (agg message)
                        (let* ((new-offset (1+ (offset message)))
                               (toppar (cons (topic message) (partition message)))
                               (current-offset (gethash toppar agg new-offset)))
                          (setf (gethash toppar agg) (max current-offset new-offset))
                          agg))
                      messages
                      :initial-value (make-hash-table :test #'equal)))
         (vector (make-array (hash-table-count hash-table)
                             :adjustable nil
                             :fill-pointer 0)))
    (maphash (lambda (k v)
               (vector-push (cons k v) vector))
             hash-table)
    (alloc-toppar-list vector :topic #'caar :partition #'cdar :offset #'cdr)))

(defmethod send-offsets-to-transaction
    ((producer producer)
     (consumer consumer)
     (offsets sequence)
     (timeout-ms integer))
  "OFFSETS should be a sequence of MESSAGES processed by CONSUMER.

This method will figure out the correct offsets to send to the
consumer group coordinator."
  (with-toppar-list toppar-list (messages->toppar-list offsets)
    (%send-offsets-to-transaction producer consumer toppar-list timeout-ms)))
