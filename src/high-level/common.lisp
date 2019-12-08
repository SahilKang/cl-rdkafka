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

(defconstant +errstr-len+ 512
  "A lot of the cl-rdkafka/low-level functions accept a char pointer and len
which is filled with an error message if anything goes wrong. This constant
determines the length of the char buffer which we'll malloc/free for such
functions.")

(defun pointer->bytes (pointer length)
  "Copies cffi :pointer bytes into a byte vector."
  (loop
     with vector = (make-array length
                               :element-type '(unsigned-byte 8)
                               :fill-pointer 0)

     for i below length
     for byte = (cffi:mem-aref pointer :uint8 i)
     do (vector-push byte vector)

     finally (return vector)))

(defun bytes->pointer (bytes)
  "Allocates and returns a new uint8 pointer to BYTES."
  (if (zerop (length bytes))
      (cffi:null-pointer)
      (cffi:foreign-alloc :uint8 :initial-contents bytes)))


(defstruct (queue (:constructor make-queue ()))
  "Good old-fashioned fifo queue."
  (head nil :read-only t :type (or null cons))
  (tail nil :read-only t :type (or null cons))
  (length 0 :read-only t :type fixnum))

(defmethod enqueue ((queue queue) item)
  "Enqueue ITEM to the end of QUEUE and return new QUEUE size."
  (with-slots (head tail length) queue
    (if tail
        (setf tail (cdr (rplacd tail (list item))))
        (setf tail (list item)
              head tail))
    (incf length)))

(defmethod dequeue ((queue queue))
  "Dequeue the next item from the front of QUEUE."
  (with-slots (head tail length) queue
    (when (zerop length)
      (error "Empty queue!"))
    (prog1 (car head)
      (setf head (cdr head))
      (decf length)
      (when (zerop length)
        (setf tail head)))))


(defun force-promise (promise)
  (declare (blackbird-base:promise promise))
  "Wait for PROMISE to complete and return value or signal condition."
  (let* ((initial-value (gensym))
         (value initial-value)
         (successp nil)
         (lock (bt:make-lock))
         (cv (bt:make-condition-variable)))
    (bb:chain promise
      (:then (result)
             (bt:with-lock-held (lock)
               (setf value result
                     successp t)
               (bt:condition-notify cv)))
      (:catch (condition)
        (bt:with-lock-held (lock)
          (setf value condition
                successp nil)
          (bt:condition-notify cv))))
    (bt:with-lock-held (lock)
      (when (eq value initial-value)
        (bt:condition-wait cv lock))
      (if successp
          value
          (error value)))))
