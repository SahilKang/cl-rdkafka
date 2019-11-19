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
  ((raw-key
    :initarg :raw-key
    :initform nil
    :documentation "Message's raw key in a byte vector.")
   (raw-value
    :initarg :raw-value
    :initform nil
    :documentation "Message's raw value in a byte vector.")
   (key
    :initarg :key
    :initform nil
    :documentation "Message's deserialized key.")
   (value
    :initarg :value
    :initform nil
    :documentation "Message's deserialized value.")
   (topic
    :initarg :topic
    :initform nil
    :reader topic
    :documentation "The topic this message originated from.")
   (partition
    :initarg :partition
    :initform nil
    :reader partition
    :documentation "The partition this message originated from.")
   (offset
    :initarg :offset
    :initform nil
    :reader offset
    :documentation "Message offset.")
   (timestamp
    :initarg :timestamp
    :initform nil
    :reader timestamp
    :documentation "Message timestamp.")
   (latency
    :initarg :latency
    :initform nil
    :reader latency
    :documentation "Message latency measured from the message produce call.")
   (headers
    :initarg :headers
    :initform nil
    :reader headers
    :documentation "Message headers as an alist.")))

(defgeneric key (message)
  (:documentation
   "Return (values deserialized-key serialized-key)."))

(defgeneric value (message)
  (:documentation
   "Return (values deserialized-value serialized-value)."))

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

(defun headers->alist (headers)
  (cffi:with-foreign-objects ((name :pointer)
                              (value :pointer)
                              (value-size 'cl-rdkafka/ll:size-t))
    (loop
       with count = (cl-rdkafka/ll:rd-kafka-header-cnt headers)

       for i below count
       for err = (cl-rdkafka/ll:rd-kafka-header-get-all
                  headers
                  i
                  name
                  value
                  value-size)
       unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
       do (error 'kafka-error
                 :description
                 (format nil "Failed to get header pair: `~A`"
                         (cl-rdkafka/ll:rd-kafka-err2str err)))

       collect (cons (cffi:mem-ref name :string)
                     (pointer->bytes
                      (cffi:mem-ref value :pointer)
                      (cffi:mem-ref value-size 'cl-rdkafka/ll:size-t))))))

(defun get-headers (rd-kafka-message)
  (cffi:with-foreign-object (headers :pointer)
    (let ((err (cl-rdkafka/ll:rd-kafka-message-headers
                rd-kafka-message
                headers)))
      (unless (or (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
                  (eq err cl-rdkafka/ll:rd-kafka-resp-err--noent))
        (error "~&Failed to get message headers: ~S"
               (cl-rdkafka/ll:rd-kafka-err2str err)))
      (when (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (headers->alist (cffi:mem-ref headers :pointer))))))

;; TODO add restarts here when serde signals a condition
(defun apply-serde (serde bytes)
  (if (functionp serde)
      (funcall serde bytes)
      bytes))

(defun rd-kafka-message->message (rd-kafka-message key-serde value-serde)
  (let* ((*rd-kafka-message (cffi:mem-ref
                             rd-kafka-message
                             '(:struct cl-rdkafka/ll:rd-kafka-message)))
         (err (getf *rd-kafka-message 'cl-rdkafka/ll:err))
         (topic (get-topic *rd-kafka-message))
         (partition (getf *rd-kafka-message 'cl-rdkafka/ll:partition)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error 'topic+partition-error
             :description (cl-rdkafka/ll:rd-kafka-err2str err)
             :topic topic
             :partition partition))
    (let ((raw-key (get-key *rd-kafka-message))
          (raw-value (get-payload *rd-kafka-message)))
      (make-instance 'message
                     :topic topic
                     :partition partition
                     :offset (getf *rd-kafka-message 'cl-rdkafka/ll:offset)
                     :timestamp (get-timestamp rd-kafka-message)
                     :latency (get-latency rd-kafka-message)
                     :headers (get-headers rd-kafka-message)
                     :raw-key raw-key
                     :raw-value raw-value
                     :key (apply-serde key-serde raw-key)
                     :value (apply-serde value-serde raw-value)))))

(defmethod key ((message message))
  (with-slots (key raw-key) message
    (values key raw-key)))

(defmethod value ((message message))
  (with-slots (value raw-value) message
    (values value raw-value)))
