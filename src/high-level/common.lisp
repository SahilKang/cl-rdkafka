;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
;;; Copyright 2023 Google LLC
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

(defun byte-seq-p (seq)
  (and (typep seq 'sequence)
       (every (lambda (elt)
                (typep elt '(unsigned-byte 8)))
              seq)))

(deftype byte-seq () '(or (vector (unsigned-byte 8)) (satisfies byte-seq-p)))
