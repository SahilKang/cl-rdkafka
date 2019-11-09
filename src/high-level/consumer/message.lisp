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
    :initform nil
    :documentation "Function to map byte vector to object, or nil for bytes.")
   (value-serde
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
   (timestamp
    :reader timestamp
    :documentation "Message timestamp.")
   (latency
    :reader latency
    :documentation "Message latency measured from the message produce call.")
   (headers
    :reader headers
    :documentation "Message headers as an alist.")))

(defgeneric key (message)
  (:documentation
   "Return message key after applying key-serde, if available."))

(defgeneric value (message)
  (:documentation
   "Return message value after applying value-serde, if available."))

(defun deref (rd-kafka-message)
  (cffi:mem-ref rd-kafka-message '(:struct cl-rdkafka/ll:rd-kafka-message)))

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
       do (error "~&Failed to get header pair: ~S"
                 (cl-rdkafka/ll:rd-kafka-err2str err))

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

(define-condition message-error (error)
  ((message-error
    :initarg :message-error
    :initform (error "Must supply message-error.")
    :reader message-error)
   (topic
    :initarg :topic
    :initform (error "Must supply topic.")
    :reader topic)
   (partition
    :initarg :partition
    :initform (error "Must supply partition.")
    :reader partition)
   (offset
    :initarg :offset
    :initform (error "Must supply offset.")
    :reader offset))
  (:report
   (lambda (c s)
     (format
      s
      "Message Error: desc: '~A' topic: '~A' partition: '~A' offset: '~A'"
      (message-error c)
      (topic c)
      (partition c)
      (offset c))))
  (:documentation
   "Condition signalled while retrieving key/value from message."))

(defmethod initialize-instance :after
    ((message message)
     &key
       (rd-kafka-message (error "Must supply pointer to rd-kafka-message."))
       serde
       key-serde
       value-serde)
  (with-slots (topic
               partition
               offset
               timestamp
               latency
               headers
               raw-key
               raw-value
               (ks key-serde)
               (vs value-serde))
      message
    (let* ((*rd-kafka-message (deref rd-kafka-message))
           (err (getf *rd-kafka-message 'cl-rdkafka/ll:err)))
      (setf topic (get-topic *rd-kafka-message)
            partition (getf *rd-kafka-message 'cl-rdkafka/ll:partition)
            offset (getf *rd-kafka-message 'cl-rdkafka/ll:offset))
      (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (error 'message-error
               :message-error (cl-rdkafka/ll:rd-kafka-err2str err)
               :topic topic
               :partition partition
               :offset offset))
      (setf timestamp (get-timestamp rd-kafka-message)
            latency (get-latency rd-kafka-message)
            headers (get-headers rd-kafka-message)
            ks (or key-serde serde)
            vs (or value-serde serde)
            raw-key (get-key *rd-kafka-message)
            raw-value (get-payload *rd-kafka-message)))))

(defmethod key ((message message))
  (with-slots (raw-key key-serde) message
    (if (functionp key-serde)
        (funcall key-serde raw-key)
        raw-key)))

(defmethod value ((message message))
  (with-slots (raw-value value-serde) message
    (if (functionp value-serde)
        (funcall value-serde raw-value)
        raw-value)))
