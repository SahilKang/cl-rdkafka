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

(defclass serde ()
  ((function
    :initarg :function
    :initform (error "Must supply function")
    :type function)
   (name
    :initarg :name
    :initform (error "Must supply name")
    :type string)))

(defgeneric apply-serde (serde arg))

(defgeneric serde-funcall (serde arg))

(defclass serializer (serde) ())

(defclass deserializer (serde) ())


(defmethod serde-funcall ((serde serializer) arg)
  (with-slots (function) serde
    (let ((serialized-value (funcall function arg)))
      (check-type serialized-value byte-seq)
      serialized-value)))

(defmethod serde-funcall ((serde deserializer) arg)
  (with-slots (function) serde
    (funcall function arg)))

(defmethod apply-serde ((serde serde) arg)
  (with-slots (function name) serde
    (restart-case
        (serde-funcall serde arg)
      (store-function (f)
        :report (lambda (stream)
                  (format stream "Store a new function for ~A and try again." name))
        :interactive (lambda ()
                       (format t "~&Type a form to be evaluated for ~A: " name)
                       (list (read)))
        (let ((f (eval f)))
          (check-type f function)
          (setf function f)
          (apply-serde serde arg))))))
