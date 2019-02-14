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

(defclass message ()
  ((rd-kafka-message
    :initarg :rd-kafka-message
    :initform (error "Must supply rd-kafka-message pointer.")
    :documentation "Pointer to rd_kafka_message_t struct.")
   (key-serde
    :initarg :key-serde
    :documentation "Function to map byte vector to object, or nil for bytes.")
   (value-serde
    :initarg :value-serde
    :documentation "Function to map byte vector to object, or nil for bytes.")
   (raw-key
    :documentation "Message's raw key in a byte vector.")
   (raw-value
    :documentation "Message's raw value in a byte vector.")
   (topic
    :reader message-topic)
   (partition
    :reader message-partition)
   (offset
    :reader message-offset)
   (kafka-error
    :reader message-error)))

(defgeneric message-key (message))
(defgeneric message-value (message))

(defun deref (rd-kafka-message)
  (cffi:mem-ref rd-kafka-message '(:struct cl-rdkafka/ll:rd-kafka-message)))

(defun get-error (*rd-kafka-message)
  (let ((err (getf *rd-kafka-message 'cl-rdkafka/ll:err)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (make-instance 'kafka-error :rd-kafka-resp-err err))))

(defun pointer->bytes (pointer length)
  (let ((vector (make-array length :element-type '(unsigned-byte 8))))
    (loop
       for i below length

       for byte = (cffi:mem-aref pointer :uint8 i)
       do (setf (elt vector i) byte))
    vector))

(defun get-payload (*rd-kafka-message)
  (let ((payload (getf *rd-kafka-message 'cl-rdkafka/ll:payload))
	(len (getf *rd-kafka-message 'cl-rdkafka/ll:len)))
    (pointer->bytes payload len)))

(defun get-key (*rd-kafka-message)
  (let ((key (getf *rd-kafka-message 'cl-rdkafka/ll:key))
	(len (getf *rd-kafka-message 'cl-rdkafka/ll:key-len)))
    (pointer->bytes key len)))

(defun get-topic (*rd-kafka-message)
  (let ((rd-kafka-topic (getf *rd-kafka-message 'cl-rdkafka/ll:topic)))
    (cl-rdkafka/ll:rd-kafka-topic-name rd-kafka-topic)))

(defmethod initialize-instance :after ((message message) &key)
  (with-slots (rd-kafka-message
	       raw-key
	       raw-value
	       topic
	       partition
	       offset
	       kafka-error) message
    (let* ((*rd-kafka-message (deref rd-kafka-message)))
      (setf kafka-error (get-error *rd-kafka-message)
	    offset (getf *rd-kafka-message 'cl-rdkafka/ll:offset)
	    partition (getf *rd-kafka-message 'cl-rdkafka/ll:partition)
	    topic (get-topic *rd-kafka-message))
      (unless kafka-error
	(setf raw-key (get-key *rd-kafka-message)
	      raw-value (get-payload *rd-kafka-message))))
    (tg:finalize
     message
     (lambda ()
       (cl-rdkafka/ll:rd-kafka-message-destroy rd-kafka-message)))))

(defmethod message-key ((message message))
  (with-slots (raw-key key-serde) message
    (unless raw-key
      (error "~&Can't access key of bad message."))
    (if (functionp key-serde)
	(funcall key-serde raw-key)
	raw-key)))

(defmethod message-value ((message message))
  (with-slots (raw-value value-serde) message
    (unless raw-value
      (error "~&Can't access value of bad message."))
    (if (functionp value-serde)
	(funcall value-serde raw-value)
	raw-value)))
