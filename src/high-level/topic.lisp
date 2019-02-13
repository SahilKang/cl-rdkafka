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

(defun make-topic (producer-or-consumer topic-name)
  (let ((rd-kafka-topic (cl-rdkafka/ll:rd-kafka-topic-new
			 producer-or-consumer
			 topic-name
			 (cffi:null-pointer))))
    (if (cffi:null-pointer-p rd-kafka-topic)
	(error "~&Failed to allocate new topic: ~A" topic-name)
	(make-instance 'topic :rd-kafka-topic rd-kafka-topic))))
