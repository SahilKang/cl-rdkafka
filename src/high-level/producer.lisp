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

(defclass producer ()
  ((rd-kafka-producer
    :documentation "Pointer to rd_kafka_t struct.")
   (topic-name->handle
    :initform (make-hash-table :test #'equal)
    :documentation "Hash-table mapping topic-names to rd-kafka-topic handles.")
   (key-serde
    :initarg :key-serde
    :initform nil
    :documentation "Function to map object to byte vector, or nil for identity.")
   (value-serde
    :initarg :value-serde
    :initform nil
    :documentation "Function to map object to byte vector, or nil for identity.")))

(defmethod initialize-instance :after ((producer producer) &key conf)
  (with-slots (rd-kafka-producer topic-name->handle) producer
    (cffi:with-foreign-object (errstr :char +errstr-len+)
      (setf rd-kafka-producer (cl-rdkafka/ll:rd-kafka-new
			       cl-rdkafka/ll:rd-kafka-producer
			       (make-conf conf)
			       errstr
			       +errstr-len+))
      (when (cffi:null-pointer-p rd-kafka-producer)
	(error "~&Failed to allocate new producer: ~A"
	       (cffi:foreign-string-to-lisp errstr :max-chars +errstr-len+))))
    (tg:finalize
     producer
     (lambda ()
       (cl-rdkafka/ll:rd-kafka-flush rd-kafka-producer (* 5 1000))
       (loop
	  for v being the hash-values of topic-name->handle
	  do (cl-rdkafka/ll:rd-kafka-topic-destroy v))
       (cl-rdkafka/ll:rd-kafka-destroy rd-kafka-producer)))))
