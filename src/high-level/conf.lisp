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

(defun alloc-rd-kafka-conf ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-conf-new)))
    (when (cffi:null-pointer-p handle)
      (error 'allocation-error :name "rd-kafka-conf"))
    handle))

(defun alloc-rd-kafka-topic-conf ()
  (let ((handle (cl-rdkafka/ll:rd-kafka-topic-conf-new)))
    (when (cffi:null-pointer-p handle)
      (error 'allocation-error :name "rd-kafka-topic-conf"))
    handle))

(defun set-rd-kafka-conf (rd-kafka-conf key value errstr errstr-len)
  (let ((result (cl-rdkafka/ll:rd-kafka-conf-set
                 rd-kafka-conf
                 key
                 value
                 errstr
                 errstr-len)))
    (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)))

(defun set-rd-kafka-topic-conf
    (rd-kafka-topic-conf key value errstr errstr-len)
  (let ((result (cl-rdkafka/ll:rd-kafka-topic-conf-set
                 rd-kafka-topic-conf
                 key
                 value
                 errstr
                 errstr-len)))
    (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)))

(defmacro make-set-keyval (old-version-p)
  `(lambda
       (rd-kafka-conf
        ,@(when old-version-p
            '(rd-kafka-topic-conf))
        key
        value
        errstr
        errstr-len)
     (unless (set-rd-kafka-conf rd-kafka-conf key value errstr errstr-len)
       ,(let ((error-form
               '(error 'kafka-error
                       :description
                       (format nil "Failed to set conf name `~A` to `~A`: `~A`"
                               key
                               value
                               (cffi:foreign-string-to-lisp
                                errstr :max-chars errstr-len)))))
          (if old-version-p
              `(unless (set-rd-kafka-topic-conf rd-kafka-topic-conf
                                                key
                                                value
                                                errstr
                                                errstr-len)
                 ,error-form)
              error-form)))))

;; newer versions of librdkafka allow topic configs to be set through
;; the same rd_kafka_conf_set function, but older versions do not. So
;; we'll implement this config fall-through ourselves.

(defmacro with-set-keyval (set-keyval &body body)
  (let* ((rd-kafka-conf (gensym))
         (rd-kafka-topic-conf (gensym))
         (errstr (gensym))
         (old-version-p (< (cl-rdkafka/ll:rd-kafka-version) #x00090500)))
    `(let (,rd-kafka-conf
           ,@(when old-version-p
               (list rd-kafka-topic-conf)))
       (handler-case
           (cffi:with-foreign-object (,errstr :char +errstr-len+)
             (flet ((,set-keyval (key value)
                      (funcall (make-set-keyval ,old-version-p)
                               ,rd-kafka-conf
                               ,@(when old-version-p
                                   (list rd-kafka-topic-conf))
                               key
                               value
                               ,errstr
                               +errstr-len+)))
               (setf ,rd-kafka-conf (alloc-rd-kafka-conf)
                     ,@(when old-version-p
                         `(,rd-kafka-topic-conf (alloc-rd-kafka-topic-conf))))
               ,@body
               ,@(when old-version-p
                   `((cl-rdkafka/ll:rd-kafka-conf-set-default-topic-conf
                      ,rd-kafka-conf
                      ,rd-kafka-topic-conf)
                     ;; rd-kafka-topic-conf is unusable at this point
                     (setf ,rd-kafka-topic-conf nil)))
               ,rd-kafka-conf))
         (condition (c)
           ,@(when old-version-p
               `((when ,rd-kafka-topic-conf
                   (cl-rdkafka/ll:rd-kafka-topic-conf-destroy ,rd-kafka-topic-conf))))
           (when ,rd-kafka-conf
             (cl-rdkafka/ll:rd-kafka-conf-destroy ,rd-kafka-conf))
           (error c))))))

(defgeneric make-conf (map))

(defmethod make-conf ((map hash-table))
  (with-set-keyval set-keyval
    (maphash (lambda (k v) (set-keyval k v)) map)))

(defun make-conf-from-alist (alist)
  (with-set-keyval set-keyval
    (loop
       for (k . v) in alist
       do (set-keyval k v))))

(defun make-conf-from-plist (plist)
  (with-set-keyval set-keyval
    (loop
       for (k v) on plist by #'cddr
       unless v
       do (error 'kafka-error
                 :description
                 (format nil "Odd number of key-val pairs: missing value for key `~A`"
                         k))
       else do (set-keyval k v))))

(defmethod make-conf ((map list))
  (etypecase (first map)
    (cons (make-conf-from-alist map))
    (string (make-conf-from-plist map))))


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
