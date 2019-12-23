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
    :type (and vector byte-seq)
    :documentation "Message's serialized key as a byte vector.")
   (raw-value
    :initarg :raw-value
    :type (and vector byte-seq)
    :documentation "Message's serialized value as a byte vector.")
   (key
    :initarg :key
    :documentation "Message's deserialized key.")
   (value
    :initarg :value
    :documentation "Message's deserialized value.")
   (topic
    :initarg :topic
    :reader topic
    :type string
    :documentation "The topic this message originated from.")
   (partition
    :initarg :partition
    :reader partition
    :type integer
    :documentation "The partition this message originated from.")
   (offset
    :initarg :offset
    :reader offset
    :type integer
    :documentation "Message offset.")
   (timestamp
    :initarg :timestamp
    :type (or null integer)
    :documentation
    "Message timestamp measured in milliseconds since the UTC epoch, or nil.")
   (timestamp-type
    :initarg :timestamp-type
    :type (member nil :create-time :log-append-time)
    :documentation "Type of message timestamp, or nil.")
   (latency
    ;; TODO this ends up being negative...figure out why and export
    :initarg :latency
    :type (or null integer)
    :documentation
    "Message latency measured in microseconds from the produce call, or nil.")
   (headers
    :initarg :headers
    :reader headers
    :type (or null list)
    :documentation "Message headers as an alist, or nil."))
  (:documentation
   "A kafka message as returned by consumer's poll or producer's produce.

Example:

(let ((message (kf:poll consumer 5000)))
  (kf:key message)
  ;; => \"key-1\", #(107 101 121 45 49)

  (kf:value message)
  ;; => \"Hello\", #(72 101 108 108 111)

  (kf:topic message)
  ;; => \"foobar\"

  (kf:partition message)
  ;; => 0

  (kf:offset message)
  ;; => 0

  (kf:timestamp message)
  ;; => 1577002478269, :CREATE-TIME

  (kf:headers message)
  ;; => '((\"one\" . #(1 2 3))
  ;;      (\"two\" . #(4 5 6)))

  )"))

(defgeneric key (message))

(defgeneric value (message))

(defgeneric timestamp (message))

(defun parse-timestamp-type (ts-type)
  (let ((*ts-type (cffi:mem-ref ts-type 'cl-rdkafka/ll:rd-kafka-timestamp-type)))
    (cond
      ((eq *ts-type 'cl-rdkafka/ll:rd-kafka-timestamp-create-time)
       :create-time)
      ((eq *ts-type 'cl-rdkafka/ll:rd-kafka-timestamp-log-append-time)
       :log-append-time)
      (t (error "Unknown timestamp-type: ~A" *ts-type)))))

(defun get-timestamp (rd-kafka-message)
  (cffi:with-foreign-object (ts-type 'cl-rdkafka/ll:rd-kafka-timestamp-type)
    (let ((timestamp (cl-rdkafka/ll:rd-kafka-message-timestamp
                      rd-kafka-message
                      ts-type)))
      (if (= -1 timestamp)
          (cons nil nil)
          (cons timestamp (parse-timestamp-type ts-type))))))

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
       do (error (make-rdkafka-error err))

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
        (error (make-rdkafka-error err)))
      (when (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (headers->alist (cffi:mem-ref headers :pointer))))))

(defun rd-kafka-message->message (rd-kafka-message key-function value-function)
  "Transform a struct rd-kafka-message pointer to a MESSAGE object.

KEY-FUNCTION and VALUE-FUNCTION are both unary functions that are
expected to output the deserialized key/value given the serialized
key/value."
  (let* ((*rd-kafka-message (cffi:mem-ref
                             rd-kafka-message
                             '(:struct cl-rdkafka/ll:rd-kafka-message)))
         (err (getf *rd-kafka-message 'cl-rdkafka/ll:err))
         (topic (get-topic *rd-kafka-message))
         (partition (getf *rd-kafka-message 'cl-rdkafka/ll:partition)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error 'partition-error
             :description (cl-rdkafka/ll:rd-kafka-err2str err)
             :topic topic
             :partition partition))
    (let ((raw-key (get-key *rd-kafka-message))
          (raw-value (get-payload *rd-kafka-message))
          (timestamp-pair (get-timestamp rd-kafka-message)))
      (make-instance 'message
                     :topic topic
                     :partition partition
                     :offset (getf *rd-kafka-message 'cl-rdkafka/ll:offset)
                     :timestamp (car timestamp-pair)
                     :timestamp-type (cdr timestamp-pair)
                     :latency (get-latency rd-kafka-message)
                     :headers (get-headers rd-kafka-message)
                     :raw-key raw-key
                     :raw-value raw-value
                     :key (funcall key-function raw-key)
                     :value (funcall value-function raw-value)))))

(defmethod key ((message message))
  "Return (values deserialized-key serialized-key) from MESSAGE."
  (with-slots (key raw-key) message
    (values key raw-key)))

(defmethod value ((message message))
  "Return (values deserialized-value serialized-value) from MESSAGE."
  (with-slots (value raw-value) message
    (values value raw-value)))

(defmethod timestamp ((message message))
  "Return (values timestamp timestamp-type) from MESSAGE.

If timestamp is not available, then nil is returned. Otherwise:
  * timestamp is the number of milliseconds since the UTC epoch
  * timestamp-type is either :create-time or :log-append-time"
  (with-slots (timestamp timestamp-type) message
    (when timestamp
      (values timestamp timestamp-type))))
