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

(define-condition kafka-error (error)
  ((description
    :initarg :description
    :initform (error "Must supply description")
    :reader description
    :type string
    :documentation
    "Hopefully some descriptive description describing the error."))
  (:report
   (lambda (condition stream)
     (format stream (description condition))))
  (:documentation
   "Generic condition signalled by cl-rdkafka for expected errors."))

(define-condition topic+partition-error (kafka-error)
  ((topic
    :initarg :topic
    :initform (error "Must supply topic")
    :reader topic
    :type string
    :documentation "Topic name.")
   (partition
    :initarg :partition
    :initform (error "Must supply partition")
    :reader partition
    :type integer
    :documentation "Topic partition."))
  (:report
   (lambda (condition stream)
     (format stream "Encountered error `~A` for topic:partition `~A:~A`"
             (description condition)
             (topic condition)
             (partition condition))))
  (:documentation
   "Condition signalled for errors specific to a topic's partition."))

(define-condition allocation-error (storage-condition)
  ((name
    :initarg :name
    :initform (error "Must supply name")
    :reader name
    :type string
    :documentation
    "Name of the object that failed to be allocated.")
   (description
    :initarg :description
    :initform nil
    :reader description
    :type string
    :documentation
    "Details about why the allocation may have failed."))
  (:report
   (lambda (condition stream)
     (format stream "Failed to allocate new `~A`~@[: `~A`~]"
             (name condition)
             (description condition))))
  (:documentation
   "Condition signalled when librdkafka functions fail to allocate pointers."))
