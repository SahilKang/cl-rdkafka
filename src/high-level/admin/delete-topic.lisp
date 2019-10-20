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
        (error "~&Failed to allocate deletetopic pointer"))
      deletetopic)))

(defun %%delete-topic (rd-kafka-client admin-options deletetopic queue)
  (cffi:with-foreign-object (deletetopic-array :pointer 1)
    (setf (cffi:mem-aref deletetopic-array :pointer 0) deletetopic)
    (cl-rdkafka/ll:rd-kafka-deletetopics rd-kafka-client
                                         deletetopic-array
                                         1
                                         admin-options
                                         queue)
    (let (event)
      (unwind-protect
           (progn
             (setf event (cl-rdkafka/ll:rd-kafka-queue-poll queue 2000))
             (when (cffi:null-pointer-p event)
               (error "~&Failed to get event from queue"))
             (assert-successful-event event deletetopics))
        (when event
          (cl-rdkafka/ll:rd-kafka-event-destroy event))))))

(defun %delete-topic (rd-kafka-client topic timeout-ms)
  (let (admin-options deletetopic queue)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 deletetopic (make-deletetopic topic)
                 queue (make-queue rd-kafka-client))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (%%delete-topic rd-kafka-client admin-options deletetopic queue))
      (when queue
        (cl-rdkafka/ll:rd-kafka-queue-destroy queue))
      (when deletetopic
        (cl-rdkafka/ll:rd-kafka-deletetopic-destroy deletetopic))
      (when admin-options
        (cl-rdkafka/ll:rd-kafka-adminoptions-destroy admin-options)))))

(macrolet
    ((defdelete (client-class)
       (let ((slot (read-from-string (format nil "rd-kafka-~A" client-class))))
         `(defmethod delete-topic
              ((client ,client-class) (topic string) &key (timeout-ms 5000))
            (with-slots (,slot) client
              (%delete-topic ,slot topic timeout-ms))
            topic))))
  (defdelete consumer)
  (defdelete producer))
