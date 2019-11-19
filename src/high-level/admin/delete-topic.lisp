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

(defgeneric delete-topic (client topic &key timeout-ms)
  (:documentation
   "Delete topic TOPIC and return TOPIC on success."))


(defun make-deletetopic (topic)
  (cffi:with-foreign-string (buf topic)
    (let ((deletetopic (cl-rdkafka/ll:rd-kafka-deletetopic-new buf)))
      (when (cffi:null-pointer-p deletetopic)
        (error 'allocation-error :name "deletetopic"))
      deletetopic)))

(defun %delete-topic (rd-kafka-client topic timeout-ms)
  (let (admin-options deletetopic)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 deletetopic (make-deletetopic topic))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (perform-admin-op deletetopics rd-kafka-client admin-options deletetopic))
      (when deletetopic
        (cl-rdkafka/ll:rd-kafka-deletetopic-destroy deletetopic))
      (when admin-options
        (cl-rdkafka/ll:rd-kafka-adminoptions-destroy admin-options)))))

(def-admin-methods
    delete-topic
    (client (topic string) &key (timeout-ms 5000))
  (%delete-topic pointer topic timeout-ms)
  topic)
