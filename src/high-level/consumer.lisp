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

(defclass consumer ()
  ((rd-kafka-consumer
    :documentation "Pointer to rd_kafka_t struct.")
   (key-serde
    :initform nil
    :documentation "Function to map byte vector to object, or nil for bytes.")
   (value-serde
    :initform nil
    :documentation "Function to map byte vector to object, or nil for bytes."))
  (:documentation
   "A client that consumes messages from kafka topics.

Example:

(let* ((string-serde (lambda (x)
                       (babel:octets-to-string x :encoding :utf-8)))
       (conf (kf:conf
              \"bootstrap.servers\" \"127.0.0.1:9092\"
              \"group.id\" \"consumer-group-id\"
              \"enable.auto.commit\" \"false\"
              \"auto.offset.reset\" \"earliest\"
              \"offset.store.method\" \"broker\"
              \"enable.partition.eof\"  \"false\"))
       (consumer (make-instance 'kf:consumer
                                :conf conf
                                :serde string-serde)))
  (kf:subscribe consumer '(\"topic-name\"))

  (loop
     for message = (kf:poll consumer (* 2 1000))
     while message

     for key = (kf:key message)
     for value = (kf:value message)

     collect (list key value)

     do (kf:commit consumer)))"))

(defgeneric subscribe (consumer topics)
  (:documentation
   "Subscribe consumer to sequence of topic names."))

(defgeneric unsubscribe (consumer)
  (:documentation
   "Unsubscribe consumer from its current topic subscription."))

(defgeneric subscription (consumer)
  (:documentation
   "Get sequence of topic names that consumer is subscribed to."))

(defgeneric poll (consumer timeout-ms)
  (:documentation
   "Block for up to timeout-ms milliseconds and return a kf:message or nil"))

(defgeneric commit (consumer &optional topic+partitions)
  (:documentation
   "Commit offsets and return either an error or nil.

If topic+partitions is nil (the default) then the current assignment is
committed."))

(defgeneric committed (consumer &optional topic+partitions)
  (:documentation
   "Get a sequence of committed topic+partitions.

If topic+partitions is nil (the default) then info about the current
assignment is returned."))

(defgeneric assignment (consumer)
  (:documentation
   "Get a sequence of assigned topic+partitions."))

(defgeneric assign (consumer topic+partitions)
  (:documentation
   "Assign partitions to consumer.

Returns nil on success or a kafka-error on failure."))

(defgeneric member-id (consumer)
  (:documentation
   "Return CONSUMER's broker-assigned group member-id."))

(defmethod initialize-instance :after
    ((consumer consumer) &key conf serde key-serde value-serde)
  (with-slots (rd-kafka-consumer (ks key-serde) (vs value-serde)) consumer
    (cffi:with-foreign-object (errstr :char +errstr-len+)
      (setf rd-kafka-consumer (cl-rdkafka/ll:rd-kafka-new
                               cl-rdkafka/ll:rd-kafka-consumer
                               (make-conf conf)
                               errstr
                               +errstr-len+))
      (when (cffi:null-pointer-p rd-kafka-consumer)
        (error "~&Failed to allocate new consumer: ~A"
               (cffi:foreign-string-to-lisp errstr :max-chars +errstr-len+))))
    (setf ks (or key-serde serde)
          vs (or value-serde serde))
    (tg:finalize
     consumer
     (lambda ()
       (cl-rdkafka/ll:rd-kafka-consumer-close rd-kafka-consumer)
       (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-consumer)))))

(defmethod subscribe ((consumer consumer) topics)
  (with-slots (rd-kafka-consumer) consumer
    (let ((rd-kafka-list (topic+partitions->rd-kafka-list
                          (map 'list
                               (lambda (name)
                                 (make-instance 'topic+partition :topic name))
                               topics))))
      (unwind-protect
           (let ((err (cl-rdkafka/ll:rd-kafka-subscribe rd-kafka-consumer
                                                        rd-kafka-list)))
             (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
               (error "~&Failed to subscribe to topics with error: ~S"
                      (cl-rdkafka/ll:rd-kafka-err2str err))))
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy rd-kafka-list)))))

(defmethod unsubscribe ((consumer consumer))
  (with-slots (rd-kafka-consumer) consumer
    (let ((err (cl-rdkafka/ll:rd-kafka-unsubscribe rd-kafka-consumer)))
      (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (error "~&Failed to unsubscribe consumer with error: ~A"
               (error-description err))))))

(defun get-topic+partitions (rd-kafka-consumer)
  (cffi:with-foreign-object (list-pointer :pointer)
    (let ((err (cl-rdkafka/ll:rd-kafka-subscription
                rd-kafka-consumer
                list-pointer)))
      (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (error "~&Failed to get subscription with error: ~A"
               (error-description err)))
      (let* ((*list-pointer (cffi:mem-ref list-pointer :pointer))
             (topic+partitions (rd-kafka-list->topic+partitions
                                *list-pointer)))
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy *list-pointer)
        topic+partitions))))

(defmethod subscription ((consumer consumer))
  (with-slots (rd-kafka-consumer) consumer
    (let ((topics (get-topic+partitions rd-kafka-consumer)))
      (loop
         for i below (length topics)
         for topic+partition = (elt topics i)
         for name = (topic topic+partition)
         do (setf (elt topics i) name))
      topics)))

(defmethod poll ((consumer consumer) (timeout-ms integer))
  (with-slots (rd-kafka-consumer key-serde value-serde) consumer
    (let (rd-kafka-message)
      (unwind-protect
           (progn
             (setf rd-kafka-message (cl-rdkafka/ll:rd-kafka-consumer-poll
                                     rd-kafka-consumer
                                     timeout-ms))
             (when (cffi:null-pointer-p rd-kafka-message)
               (setf rd-kafka-message nil))
             (when rd-kafka-message
               (make-instance 'message
                              :rd-kafka-message rd-kafka-message
                              :key-serde key-serde
                              :value-serde value-serde)))
        (when rd-kafka-message
          (cl-rdkafka/ll:rd-kafka-message-destroy rd-kafka-message))))))

;; TODO signal a condition instead of returning kafka-error
(defun %commit (rd-kafka-consumer rd-kafka-topic-partition-list)
  (unwind-protect
       (let ((err (cl-rdkafka/ll:rd-kafka-commit
                   rd-kafka-consumer
                   rd-kafka-topic-partition-list
                   0)))
         (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
           (make-instance 'kafka-error :rd-kafka-resp-err err)))
    (unless (cffi:null-pointer-p rd-kafka-topic-partition-list)
      (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy
       rd-kafka-topic-partition-list))))

(defmethod commit ((consumer consumer) &optional topic+partitions)
  (with-slots (rd-kafka-consumer) consumer
    (if topic+partitions
        (%commit rd-kafka-consumer
                 (topic+partitions->rd-kafka-list topic+partitions))
        (%commit rd-kafka-consumer
                 (cffi:null-pointer)))))

(defun %assignment (rd-kafka-consumer)
  (cffi:with-foreign-object (rd-list :pointer)
    (let ((err (cl-rdkafka/ll:rd-kafka-assignment rd-kafka-consumer rd-list)))
      (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (error "~&Failed to get assignment: ~S"
               (cl-rdkafka/ll:rd-kafka-err2str err)))
      (cffi:mem-ref rd-list :pointer))))

(defmethod assignment ((consumer consumer))
  (with-slots (rd-kafka-consumer) consumer
    (let ((rd-list (%assignment rd-kafka-consumer)))
      (unwind-protect
           (rd-kafka-list->topic+partitions rd-list)
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy rd-list)))))

(defmethod committed ((consumer consumer) &optional topic+partitions)
  (with-slots (rd-kafka-consumer) consumer
    (let ((rd-list (if topic+partitions
                       (topic+partitions->rd-kafka-list topic+partitions)
                       (%assignment rd-kafka-consumer))))
      (unwind-protect
           (let ((err (cl-rdkafka/ll:rd-kafka-committed
                       rd-kafka-consumer
                       rd-list
                       60000)))
             (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
               (error "~&Failed to get committed offsets with error: ~S"
                      (cl-rdkafka/ll:rd-kafka-err2str err)))
             (rd-kafka-list->topic+partitions rd-list))
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy rd-list)))))

;; TODO create and signal a condition (get rid of kafka-error class)
(defmethod assign ((consumer consumer) topic+partitions)
  (with-slots (rd-kafka-consumer) consumer
    (let ((rd-list (topic+partitions->rd-kafka-list topic+partitions)))
      (unwind-protect
           (let ((err (cl-rdkafka/ll:rd-kafka-assign rd-kafka-consumer rd-list)))
             (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
               (make-instance 'kafka-error :rd-kafka-resp-err err)))
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy rd-list)))))

(defmethod member-id ((consumer consumer))
  (with-slots (rd-kafka-consumer) consumer
    (cl-rdkafka/ll:rd-kafka-memberid rd-kafka-consumer)))
