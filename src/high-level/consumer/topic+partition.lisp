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
    :initform (error "Must supply topic name.")
    :documentation "Topic name.")
   (partition
    :reader partition
    :initarg :partition
    :initform -1
    :documentation "Topic partition.")
   (offset
    :reader offset
    :initarg :offset
    :initform cl-rdkafka/ll:rd-kafka-offset-invalid
    :documentation "Topic offset.")
   (metadata
    :reader metadata
    :initarg :metadata
    :initform nil
    :documentation "Topic metadata."))
  (:documentation
   "Holds info for topic, partition, offset, and metadata."))

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
        (set-field 'cl-rdkafka/ll:metadata
                   (cffi:foreign-alloc :uint8 :initial-contents metadata))
        (set-field 'cl-rdkafka/ll:metadata-size (length metadata))))
    elem))

(defun topic+partitions->rd-kafka-list (topic+partitions)
  "Returns a pointer to a newly allocated
 cl-rdkafka/ll:rd-kafka-topic-partition-list."
  (let ((rd-list (cl-rdkafka/ll:rd-kafka-topic-partition-list-new
                  (length topic+partitions))))
    (when (cffi:null-pointer-p rd-list)
      (error "~&Failed to allocate new rd-kafka-topic-partition-list"))
    (map nil (lambda (t+p) (add-topic+partition rd-list t+p)) topic+partitions)
    rd-list))

(defun parse-metadata (rd-kafka-topic-partition)
  (let ((metadata (getf rd-kafka-topic-partition 'cl-rdkafka/ll:metadata))
        (length (getf rd-kafka-topic-partition 'cl-rdkafka/ll:metadata-size)))
    (unless (cffi:null-pointer-p metadata)
      (pointer->bytes metadata length))))

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

(defmacro foreach-toppar (toppar-list (&rest fields) &body body)
  "For each element in TOPPAR-LIST, BODY is evaluated under FIELDS bindings.

The symbols in FIELDS are bound to the corresponding fields of each
TOPPAR-LIST element.

TOPPAR-LIST should be a pointer to a
cl-rdkafka/ll:rd-kafka-topic-partition-list."
  (let* ((*toppar-list (gensym))
         (elems (gensym))
         (elem (gensym))
         (count (gensym))
         (i (gensym))
         (field-bindings (mapcar
                          (lambda (symbol)
                            (let ((field (find-symbol (string symbol)
                                                      'cl-rdkafka/ll)))
                              (unless field
                                (error "~&Could not find symbol for ~S" symbol))
                              `(,symbol (getf ,elem ',field))))
                          fields)))
    `(loop
        with ,*toppar-list = (cffi:mem-ref
                              ,toppar-list
                              '(:struct cl-rdkafka/ll:rd-kafka-topic-partition-list))
        with ,elems = (getf ,*toppar-list 'cl-rdkafka/ll:elems)
        with ,count = (getf ,*toppar-list 'cl-rdkafka/ll:cnt)

        for ,i below ,count
        for ,elem = (cffi:mem-aref
                     ,elems
                     '(:struct cl-rdkafka/ll:rd-kafka-topic-partition)
                     ,i)

        do (let ,field-bindings
             ,@body))))
