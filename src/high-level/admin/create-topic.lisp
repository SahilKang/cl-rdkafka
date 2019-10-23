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

(defgeneric create-topic
    (client topic
     &key partitions replication-factor conf timeout-ms validate-only-p)
  (:documentation
   "Create topic TOPIC with given keyword args and return TOPIC on success.

If VALIDATE-ONLY-P is true, then the create topic request will be
validated by the broker without the topic actually being created."))


(defun make-newtopic
    (topic-name partitions replication-factor errstr errstr-len)
  (cffi:with-foreign-string (buf topic-name)
    (let ((newtopic (cl-rdkafka/ll:rd-kafka-newtopic-new
                     buf
                     partitions
                     replication-factor
                     errstr
                     errstr-len)))
      (when (cffi:null-pointer-p newtopic)
        (error "~&Failed to allocate newtopic pointer: ~S"
               (cffi:foreign-string-to-lisp errstr :max-chars (1- errstr-len))))
      newtopic)))

(defun set-conf (newtopic conf)
  (flet ((set-kv (k v)
           (let ((err (cl-rdkafka/ll:rd-kafka-newtopic-set-config
                       newtopic
                       k
                       v)))
             (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
               (error "~&Error ~S when setting config for key-val: ~S => ~S"
                      (cl-rdkafka/ll:rd-kafka-err2str err)
                      k
                      v)))))
    (loop
       for (k v) on conf by #'cddr
       unless v
       do (error "~&Odd number of key-val pairs: missing value for key: ~S" k)
       else do (set-kv k v))))

(defun %create-topic
    (rd-kafka-client
     topic
     partitions
     replication-factor
     conf
     timeout-ms
     validate-only-p)
  (let (admin-options newtopic)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 newtopic (make-newtopic topic
                                         partitions
                                         replication-factor
                                         errstr
                                         +errstr-len+))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (set-validate admin-options validate-only-p errstr +errstr-len+)
           (set-conf newtopic conf)
           (perform-admin-op createtopics rd-kafka-client admin-options newtopic))
      (when newtopic
        (cl-rdkafka/ll:rd-kafka-newtopic-destroy newtopic))
      (when admin-options
        (cl-rdkafka/ll:rd-kafka-adminoptions-destroy admin-options)))))

(def-admin-methods
    create-topic
    (client
     (topic string)
     &key
     (partitions 1)
     (replication-factor 1)
     conf
     (timeout-ms 5000)
     (validate-only-p nil))
  (%create-topic pointer
                 topic
                 partitions
                 replication-factor
                 conf
                 timeout-ms
                 validate-only-p)
  topic)
