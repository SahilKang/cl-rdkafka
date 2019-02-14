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

(defclass topic+partition ()
  ((topic
    :reader topic
    :initarg :topic
    :initform (error "Must supply topic name."))
   (partition
    :reader partition
    :initarg :partition
    :initform -1)
   (offset
    :reader offset
    :initarg :offset
    :initform cl-rdkafka/ll:rd-kafka-offset-invalid)
   (metadata
    :reader metadata
    :initarg :metadata
    :initform nil)))

(defun add-topic+partition (rd-kafka-list topic+partition)
  (let ((elem (cl-rdkafka/ll:rd-kafka-topic-partition-list-add
	       rd-kafka-list
	       (topic topic+partition)
	       (partition topic+partition)))
	(offset (offset topic+partition))
	(metadata (metadata topic+partition)))
    (flet ((set-field (field value)
	     (setf (cffi:foreign-slot-value
		    elem
		    '(:struct cl-rdkafka/ll:rd-kafka-topic-partition)
		    field)
		   value)))
      (set-field 'cl-rdkafka/ll:offset offset)
      (when metadata
	(let ((bytes (object->bytes metadata)))
	  (set-field 'cl-rdkafka/ll:metadata
		     (cffi:foreign-alloc :uint8 :initial-contents bytes))
	  (set-field 'cl-rdkafka/ll:metadata-size (length bytes)))))
    elem))

(defun topic+partitions->rd-kafka-list (topic+partitions)
  "Returns a pointer to a newly allocated
 cl-rdkafka/ll:rd-kafka-topic-partition-list."
  (let ((rd-list (cl-rdkafka/ll:rd-kafka-topic-partition-list-new
		  (length topic+partitions))))
    (map nil (lambda (t+p) (add-topic+partition rd-list t+p)) topic+partitions)
    rd-list))

(defun parse-metadata (rd-kafka-topic-partition)
  (let ((metadata (getf rd-kafka-topic-partition 'cl-rdkafka/ll:metadata))
	(length (getf rd-kafka-topic-partition 'cl-rdkafka/ll:metadata-size)))
    (unless (cffi:null-pointer-p metadata)
      (bytes->object (pointer->bytes metadata length) 'string))))

(defun struct->topic+partition (rd-kafka-topic-partition)
  (let ((topic (getf rd-kafka-topic-partition 'cl-rdkafka/ll:topic))
	(partition (getf rd-kafka-topic-partition 'cl-rdkafka/ll:partition))
	(offset (getf rd-kafka-topic-partition 'cl-rdkafka/ll:offset))
	(metadata (parse-metadata rd-kafka-topic-partition)))
    (make-instance 'topic+partition
		   :topic topic
		   :partition partition
		   :offset offset
		   :metadata metadata)))

(defun rd-kafka-list->topic+partitions (rd-kafka-list)
  (let* ((*rd-kafka-list
	  (cffi:mem-ref
	   rd-kafka-list
	   '(:struct cl-rdkafka/ll:rd-kafka-topic-partition-list)))
	 (elems (getf *rd-kafka-list 'cl-rdkafka/ll:elems))
	 (length (getf *rd-kafka-list 'cl-rdkafka/ll:cnt))
	 (vector (make-array length :element-type 'topic+partition)))
    (loop
       for i below length
       for elem = (cffi:mem-aref
		   elems
		   '(:struct cl-rdkafka/ll:rd-kafka-topic-partition)
		   i)
       for topic+partition = (struct->topic+partition elem)
       do (setf (elt vector i) topic+partition))
    vector))
