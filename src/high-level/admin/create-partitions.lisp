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

(defgeneric create-partitions
    (client topic partitions &key timeout-ms)
  (:documentation
   "Increase TOPIC's partition count up to PARTITIONS and return PARTITIONS on success."))


(defun make-newpart (topic partitions errstr errstr-len)
  (cffi:with-foreign-string (buf topic)
    (let ((newpart (cl-rdkafka/ll:rd-kafka-newpartitions-new
                    buf
                    partitions
                    errstr
                    errstr-len)))
      (when (cffi:null-pointer-p newpart)
        (error "~&Failed to allocated newpart pointer: ~S"
               (cffi:foreign-string-to-lisp errstr :max-chars (1- errstr-len))))
      newpart)))

(defun %create-partitions
    (rd-kafka-client topic partitions timeout-ms)
  (let (admin-options newpart)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 newpart (make-newpart topic partitions errstr +errstr-len+))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (perform-admin-op createpartitions rd-kafka-client admin-options newpart))
      (when newpart
        (cl-rdkafka/ll:rd-kafka-newpartitions-destroy newpart))
      (when admin-options
        (cl-rdkafka/ll:rd-kafka-adminoptions-destroy admin-options)))))

(def-admin-methods
    create-partitions
    (client (topic string) (partitions fixnum) &key (timeout-ms 5000))
  (%create-partitions pointer topic partitions timeout-ms)
  partitions)
