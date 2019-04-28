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
  ((key-serde
    :initarg :key-serde
    :initform nil
    :documentation "Function to map byte vector to object, or nil for bytes.")
   (value-serde
    :initarg :value-serde
    :initform nil
    :documentation "Function to map byte vector to object, or nil for bytes.")
   (raw-key
    :reader raw-key
    :initform nil
    :documentation "Message's raw key in a byte vector.")
   (raw-value
    :reader raw-value
    :initform nil
    :documentation "Message's raw value in a byte vector.")
   (topic
    :reader topic
    :documentation "The topic this message originated from.")
   (partition
    :reader partition
    :documentation "The partition this message originated from.")
   (offset
    :reader offset
    :documentation "Message offset.")
   (message-error
    :reader message-error
    :documentation "Message error, if any.")
   (timestamp
    :reader timestamp
    :documentation "Message timestamp.")
   (latency
    :reader latency
    :documentation "Message latency measured from the message produce call.")))

(defgeneric key (message)
  (:documentation
   "Return message key after applying key-serde,if available."))

(defgeneric value (message)
  (:documentation
   "Return message value after applying value-serde, if available."))

(defun deref (rd-kafka-message)
  (cffi:mem-ref rd-kafka-message '(:struct cl-rdkafka/ll:rd-kafka-message)))

(defun get-error (*rd-kafka-message)
  (let ((err (getf *rd-kafka-message 'cl-rdkafka/ll:err)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (make-instance 'kafka-error :rd-kafka-resp-err err))))

(defun get-timestamp (rd-kafka-message)
  (cffi:with-foreign-object (ts-type 'cl-rdkafka/ll:rd-kafka-timestamp-type)
    (let ((timestamp (cl-rdkafka/ll:rd-kafka-message-timestamp
		      rd-kafka-message
		      ts-type)))
      (unless (= -1 timestamp)
	timestamp))))

(defun get-latency (rd-kafka-message)
  (handler-case
      (let ((latency (cl-rdkafka/ll:rd-kafka-message-latency rd-kafka-message)))
	(unless (= -1 latency)
	  latency))
    (serious-condition () nil)))

(defun get-topic (*rd-kafka-message)
  (let ((rd-kafka-topic (getf *rd-kafka-message 'cl-rdkafka/ll:rkt)))
    (cl-rdkafka/ll:rd-kafka-topic-name rd-kafka-topic)))

(defun get-key (*rd-kafka-message)
  (let ((key (getf *rd-kafka-message 'cl-rdkafka/ll:key))
	(len (getf *rd-kafka-message 'cl-rdkafka/ll:key-len)))
    (pointer->bytes key len)))

(defun get-payload (*rd-kafka-message)
  (let ((payload (getf *rd-kafka-message 'cl-rdkafka/ll:payload))
	(len (getf *rd-kafka-message 'cl-rdkafka/ll:len)))
    (pointer->bytes payload len)))

(defmethod initialize-instance :after
    ((message message)
     &key (rd-kafka-message (error "Must supply pointer to rd-kafka-message.")))
  (with-slots (message-error
      	       topic
	       partition
	       offset
	       timestamp
	       latency
	       raw-key
	       raw-value) message
    (let ((*rd-kafka-message (deref rd-kafka-message)))
      (setf message-error (get-error *rd-kafka-message)
	    topic (get-topic *rd-kafka-message)
	    partition (getf *rd-kafka-message 'cl-rdkafka/ll:partition)
	    offset (getf *rd-kafka-message 'cl-rdkafka/ll:offset)
	    timestamp (get-timestamp rd-kafka-message)
	    latency (get-latency rd-kafka-message))
      (unless message-error
	(setf raw-key (get-key *rd-kafka-message)
	      raw-value (get-payload *rd-kafka-message))))))

(defmethod key ((message message))
  (with-slots (raw-key key-serde) message
    (unless raw-key
      (error "~&Can't access key of bad message."))
    (if (functionp key-serde)
	(funcall key-serde raw-key)
	raw-key)))

(defmethod value ((message message))
  (with-slots (raw-value value-serde) message
    (unless raw-value
      (error "~&Can't access value of bad message."))
    (if (functionp value-serde)
	(funcall value-serde raw-value)
	raw-value)))
