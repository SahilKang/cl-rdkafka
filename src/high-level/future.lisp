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

(defclass future ()
  ((thread
    :initform nil)
   (value
    :initform nil))
  (:documentation
   "An object to represent the result of an async operation.

Example:

(let ((result (make-instance
	       'kf:future
	       :thunk (lambda ()
			(sleep 5)
			\"Hello World!\"))))
  ;; do some other stuff while result future is being computed in background
  (format t \"~&result from background: ~A\" (kf:value result)))"))

(defgeneric value (future)
  (:documentation
   "Block until computation is complete and return the value."))

(defmethod initialize-instance :after
    ((future future)
     &key (thunk (error "Must supply thunk.")))
  (with-slots (thread value) future
    (setf thread (bt:make-thread
		  (lambda ()
		    (declare (special value))
		    (setf value (funcall thunk)))))))

(defmethod value ((future future))
  (with-slots (thread value) future
    (bt:join-thread thread)
    value))
