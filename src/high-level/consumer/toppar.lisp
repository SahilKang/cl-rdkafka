;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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


(defun add-toppar (toppar-list topic partition offset metadata)
  (let ((toppar (cl-rdkafka/ll:rd-kafka-topic-partition-list-add
                 toppar-list
                 topic
                 partition)))
    (flet ((set-field (field value)
             (setf (cffi:foreign-slot-value
                    toppar
                    '(:struct cl-rdkafka/ll:rd-kafka-topic-partition)
                    field)
                   value)))
      (set-field 'cl-rdkafka/ll:offset offset)
      (when metadata
        (set-field 'cl-rdkafka/ll:metadata (bytes->pointer metadata))
        (set-field 'cl-rdkafka/ll:metadata-size (length metadata))))
    toppar))

(defun alloc-toppar-list
    (seq
     &key
       (topic #'identity)
       (partition (lambda (x)
                    (declare (ignore x))
                    -1))
       (offset (lambda (x)
                 (declare (ignore x))
                 cl-rdkafka/ll:rd-kafka-offset-invalid))
       (metadata (lambda (x)
                   (declare (ignore x))
                   nil)))
  "Returns a newly allocated
cl-rdkafka/ll:rd-kafka-topic-partition-list initialized with the
elements in SEQ.

The keyword args denote functions which will be applied to each
element of SEQ to extract the corresponding
cl-rdkafka/ll:rd-kafka-topic-partition struct field."
  (let ((toppar-list (cl-rdkafka/ll:rd-kafka-topic-partition-list-new
                      (length seq))))
    (when (cffi:null-pointer-p toppar-list)
      (error 'allocation-error :name "rd-kafka-topic-partition-list"))
    (handler-case
        (flet ((add-toppar (x)
                 (add-toppar toppar-list
                             (funcall topic x)
                             (funcall partition x)
                             (funcall offset x)
                             (funcall metadata x))))
          (map nil #'add-toppar seq)
          toppar-list)
      (condition (c)
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy toppar-list)
        (error c)))))


(defmacro with-toppar-list (symbol alloc-form &body body)
  `(let ((,symbol ,alloc-form))
     (unwind-protect
          (progn
            ,@body)
       (unless (or (null ,symbol) (cffi:null-pointer-p ,symbol))
         (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy ,symbol)))))
