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

(defclass conf ()
  ((rd-kafka-conf
    :initform nil
    :documentation "Pointer to rd_kafka_conf_t struct.")
   (rd-kafka-topic-conf
    :initform nil
    :documentation "Pointer to rd_kafka_topic_conf_t struct.")
   (merge-confs-p
    :initform nil
    :documentation
    "Determines if the two confs should be merged.
This is set to true only when the fall-through function is needed.")))

(defgeneric (setf prop) (prop-value conf prop-key))

(defgeneric prop (conf prop-key))

(defgeneric rd-kafka-conf (conf))

(defgeneric destroy-conf (conf))

(defun new-conf ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-conf-new)))
    (when (cffi:null-pointer-p handle)
      (error "~&Failed to allocate new rd-kafka-conf"))
    handle))

(defun new-topic-conf ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-topic-conf-new)))
    (when (cffi:null-pointer-p handle)
      (error "~&Failed to allocate new rd-kafka-topic-conf"))
    handle))

(defmethod initialize-instance :after ((conf conf) &key)
  (with-slots (rd-kafka-conf rd-kafka-topic-conf) conf
    (handler-case
        (setf rd-kafka-conf (new-conf)
              rd-kafka-topic-conf (new-topic-conf))
      (condition (c)
        (when rd-kafka-conf
          (cl-rdkafka/ll:rd-kafka-conf-destroy rd-kafka-conf))
        (when rd-kafka-topic-conf
          (cl-rdkafka/ll:rd-kafka-topic-conf-destroy rd-kafka-topic-conf))
        (error c)))))


(defgeneric make-conf (map))

(defmethod make-conf ((map hash-table))
  (let ((conf (make-instance 'conf)))
    (handler-case
        (progn
          (maphash (lambda (k v) (setf (prop conf k) v)) map)
          (rd-kafka-conf conf))
      (condition (c)
        (destroy-conf conf)
        (error c)))))

(defun make-conf-from-alist (alist)
  (let ((conf (make-instance 'conf)))
    (handler-case
        (loop
           for (k . v) in alist
           do (setf (prop conf k) v)

           finally (return (rd-kafka-conf conf)))
      (condition (c)
        (destroy-conf conf)
        (error c)))))

(defun make-conf-from-plist (plist)
  (loop
     with alist = nil

     for (k v) on plist by #'cddr
     unless v
     do (error "~&Odd number of key-val pairs: missing value for key ~S" k)
     else do (push (cons k v) alist)

     finally (return (make-conf-from-alist alist))))

(defmethod make-conf ((map list))
  (etypecase (first map)
    (cons (make-conf-from-alist map))
    (string (make-conf-from-plist map))))

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
    (unless (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
      (error "~&Failed to set conf name ~S to ~S: ~S"
             prop-key
             prop-value
             (cffi:foreign-string-to-lisp errstr :max-chars errstr-len)))
    prop-value))

(defmethod (setf prop) ((prop-value string) (conf conf) (prop-key string))
  (with-slots (rd-kafka-conf rd-kafka-topic-conf merge-confs-p) conf
    (cffi:with-foreign-object (errstr :char +errstr-len+)
      (let ((result (cl-rdkafka/ll:rd-kafka-conf-set
                     rd-kafka-conf
                     prop-key
                     prop-value
                     errstr
                     +errstr-len+)))
        (unless (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
          (fall-through rd-kafka-topic-conf
                        prop-key
                        prop-value
                        errstr
                        +errstr-len+)
          ;; fall-through was successful so we're running an older
          ;; version of librdkafka and need to merge the two confs
          ;; during the rd-kafka-conf call
          (setf merge-confs-p t)))))
  prop-value)

(defun rise-through (rd-kafka-topic-conf prop-key prop-value len)
  (let ((result (cl-rdkafka/ll:rd-kafka-topic-conf-get
                 rd-kafka-topic-conf
                 prop-key
                 prop-value
                 len)))
    (cond
      ((eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
       (cffi:foreign-string-to-lisp
        prop-value
        :max-chars (cffi:mem-ref len 'cl-rdkafka/ll:size-t)))
      ((eq result 'cl-rdkafka/ll:rd-kafka-conf-unknown)
       (error "~&Unknown conf name: ~S" prop-key))
      (t
       (error "~&Unexpected result when getting prop-key ~S: ~S"
              prop-key
              result)))))

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
           (error "~&Unexpected result when getting prop-key ~S: ~S"
                  prop-key
                  result)))))))

(defmethod rd-kafka-conf ((conf conf))
  (with-slots (rd-kafka-conf rd-kafka-topic-conf merge-confs-p) conf
    ;; merge the two confs if needed, or destroy the topic-conf if
    ;; not.  rd-kafka-topic-conf is unusable after either one of these
    ;; calls so it's set to nil afterwards.
    (if merge-confs-p
        (cl-rdkafka/ll:rd-kafka-conf-set-default-topic-conf
         rd-kafka-conf
         rd-kafka-topic-conf)
        (cl-rdkafka/ll:rd-kafka-topic-conf-destroy rd-kafka-topic-conf))
    (setf rd-kafka-topic-conf nil)
    rd-kafka-conf))

(defmethod destroy-conf ((conf conf))
  (with-slots (rd-kafka-conf rd-kafka-topic-conf) conf
    (when rd-kafka-conf
      (cl-rdkafka/ll:rd-kafka-conf-destroy rd-kafka-conf)
      (setf rd-kafka-conf nil))
    (when rd-kafka-topic-conf
      (cl-rdkafka/ll:rd-kafka-topic-conf-destroy rd-kafka-topic-conf)
      (setf rd-kafka-topic-conf nil))))


(defmacro with-conf (conf-pointer conf-mapping &body body)
  "Binds CONF-POINTER to output of CONF-MAPPING during BODY.

If BODY evaluates without signalling a condition, then it is expected
to take ownership of CONF-POINTER. If a condition is signalled by
BODY, however, then CONF-POINTER will be freed by this macro."
  `(let ((,conf-pointer (make-conf ,conf-mapping)))
     (handler-case
         (progn
           ,@body)
       (condition (c)
         (cl-rdkafka/ll:rd-kafka-conf-destroy ,conf-pointer)
         (error c)))))
