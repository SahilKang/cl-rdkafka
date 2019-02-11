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

(defvar +errstr-len+ 512)

(defclass topic ()
  ((rd-kafka-topic
    :initarg :rd-kafka-topic
    :initform (error "Must supply rd-kafka-topic pointer.")
    :documentation "Pointer to rd_kafka_topic_t struct.")
   (name
    :reader topic-name)))

(defmethod initialize-instance :after ((topic topic) &key)
  (with-slots (rd-kafka-topic name) topic
    (setf name (cl-rdkafka/ll:rd-kafka-topic-name rd-kafka-topic))
    (tg:finalize
     topic
     (lambda ()
       (cl-rdkafka/ll:rd-kafka-topic-destroy rd-kafka-topic)))))

(defun ->topic-conf (topic-props)
  (if topic-props
      (let ((topic-conf (make-instance 'topic-conf)))
	(maphash (lambda (k v) (set-prop topic-conf k v)) topic-props)
	topic-conf)
      (cffi:null-pointer)))

(defun make-topic (producer-or-consumer topic-name topic-props)
  (let ((rd-kafka-topic (cl-rdkafka/ll:rd-kafka-topic-new
			 producer-or-consumer
			 topic-name
			 (->topic-conf topic-props))))
    (if (cffi:null-pointer-p rd-kafka-topic)
	(error "~&Failed to allocate new topic: ~A" topic-name)
	(make-instance 'topic :rd-kafka-topic rd-kafka-topic))))

;;; topic-conf

(defclass topic-conf ()
  ((rd-kafka-topic-conf
    :initarg :rd-kafka-topic-conf
    :documentation "Pointer to rd_kafka_topic_conf_t struct.")))

(defgeneric set-prop (topic-conf key value))

(defun new-handle ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-topic-conf-new)))
    (if (cffi:null-pointer-p handle)
	(error "~&Function ~A failed to allocate new rd-kafka-topic-conf"
	       'cl-rdkafka/ll:rd-kafka-topic-conf-new)
	handle)))

(defmethod initialize-instance :after ((topic-conf topic-conf) &key)
  (with-slots (rd-kafka-topic-conf) topic-conf
    (setf rd-kafka-topic-conf (new-handle))
    (tg:finalize
     topic-conf
     (lambda ()
       (cl-rdkafka/ll:rd-kafka-topic-conf-destroy rd-kafka-topic-conf)))))

(defmethod set-prop ((topic-conf topic-conf) (key string) (value string))
  (with-slots (rd-kafka-topic-conf) topic-conf
    (cffi:with-foreign-object (errstr :char +errstr-len+)
      (let ((result (cl-rdkafka/ll:rd-kafka-topic-conf-set
		     rd-kafka-topic-conf
		     key
		     value
		     errstr
		     +errstr-len+)))
	(unless (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
	  (error "~&Failed to set topic-conf property ~A=~A with error: ~A"
		 key
		 value
		 result))))))
