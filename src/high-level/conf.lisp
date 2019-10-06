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

(defconstant +prop-value-len+ 512
  "The maximum byte length of strings returned by (prop conf prop-key).")

(defun new-conf ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-conf-new)))
    (if (cffi:null-pointer-p handle)
        (error "~&Function ~A failed to allocate new rd-kafka-conf"
               'cl-rdkafka/ll:rd-kafka-conf-new)
        handle)))

(defun new-topic-conf ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-topic-conf-new)))
    (if (cffi:null-pointer-p handle)
        (error "~&Function ~A failed to allocate new rd-kafka-topic-conf"
               'cl-rdkafka/ll:rd-kafka-topic-conf-new)
        handle)))

;; TODO add use-value condition handler
(defun conf (&rest key-vals)
  "Construct a hash-table from the strings in key-vals."
  (let ((h (make-hash-table :test #'equal)))
    (loop
       for (k v) on key-vals
       by #'cddr
       unless v
       do (error "~&Odd number of key-val pairs: missing value for key `~A`" k)
       else do (setf (gethash k h) v))
    h))

(defclass conf ()
  ((rd-kafka-conf
    :initform (new-conf)
    :documentation "Pointer to rd_kafka_conf_t struct.")
   (rd-kafka-topic-conf
    :initform (new-topic-conf)
    :documentation "Pointer to rd_kafka_topic_conf_t struct.")))

(defgeneric (setf prop) (prop-value conf prop-key))

(defgeneric prop (conf prop-key))

(defgeneric rd-kafka-conf (conf))

(defun make-conf (hash-table)
  (if hash-table
      (let ((conf (make-instance 'conf)))
        (maphash (lambda (k v) (setf (prop conf k) v)) hash-table)
        (rd-kafka-conf conf))
      (cffi:null-pointer)))

;; newer versions of librdkafka allow topic configs to be set through
;; the same rd_kafka_conf_set function, but older versions do not. So
;; we'll implement this config fall-through ourselves.

(defun fall-through (rd-kafka-topic-conf prop-key prop-value errstr errstr-len)
  (let ((result (cl-rdkafka/ll:rd-kafka-topic-conf-set
                 rd-kafka-topic-conf
                 prop-key
                 prop-value
                 errstr
                 errstr-len)))
    (when (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
      prop-value)))

(defmethod (setf prop) ((prop-value string) (conf conf) (prop-key string))
  (with-slots (rd-kafka-conf rd-kafka-topic-conf) conf
    (cffi:with-foreign-object (errstr :char +errstr-len+)
      (let ((result (cl-rdkafka/ll:rd-kafka-conf-set
                     rd-kafka-conf
                     prop-key
                     prop-value
                     errstr
                     +errstr-len+)))
        (if (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
            prop-value
            (fall-through rd-kafka-topic-conf
                          prop-key
                          prop-value
                          errstr
                          +errstr-len+))))))

(defun rise-through (rd-kafka-topic-conf prop-key prop-value len)
  (let ((result (cl-rdkafka/ll:rd-kafka-topic-conf-get
                 rd-kafka-topic-conf
                 prop-key
                 prop-value
                 len)))
    (when (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
      (cffi:foreign-string-to-lisp
       prop-value
       :max-chars (cffi:mem-ref len 'cl-rdkafka/ll:size-t)))))

(defmethod prop ((conf conf) (prop-key string))
  (with-slots (rd-kafka-conf rd-kafka-topic-conf) conf
    (cffi:with-foreign-objects
        ((prop-value :char +prop-value-len+)
         (len 'cl-rdkafka/ll:size-t))

      (setf (cffi:mem-ref len 'cl-rdkafka/ll:size-t) +prop-value-len+)

      (let ((result (cl-rdkafka/ll:rd-kafka-conf-get
                     rd-kafka-conf
                     prop-key
                     prop-value
                     len)))
        (cond
          ((eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
           (cffi:foreign-string-to-lisp
            prop-value
            :max-chars (cffi:mem-ref len 'cl-rdkafka/ll:size-t)))

          ((eq result 'cl-rdkafka/ll:rd-kafka-conf-unknown)
           (rise-through rd-kafka-topic-conf prop-key prop-value len))

          (t
           (error "~&Unexpected result when getting prop-key ~A: ~A"
                  prop-key
                  result)))))))

(defmethod rd-kafka-conf ((conf conf))
  (with-slots (rd-kafka-conf rd-kafka-topic-conf) conf
    (cl-rdkafka/ll:rd-kafka-conf-set-default-topic-conf
     rd-kafka-conf
     rd-kafka-topic-conf)
    (setf rd-kafka-topic-conf nil) ; unusable after set-default-topic-conf call
    rd-kafka-conf))
