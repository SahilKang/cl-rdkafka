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

(defun make-admin-options (rd-kafka-client)
  (let ((admin-options (cl-rdkafka/ll:rd-kafka-adminoptions-new
                        rd-kafka-client
                        cl-rdkafka/ll:rd-kafka-admin-op-any)))
    (when (cffi:null-pointer-p admin-options)
      (error "~&Failed to allocate new admin-options pointer"))
    admin-options))

(defun set-timeout (admin-options timeout-ms errstr errstr-len)
  (let ((err (cl-rdkafka/ll:rd-kafka-adminoptions-set-request-timeout
              admin-options
              timeout-ms
              errstr
              errstr-len)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error "~&Failed to set request timeout of admin-options: ~S"
             (cffi:foreign-string-to-lisp errstr :max-chars (1- errstr-len))))))

(defun set-validate (admin-options validatep errstr errstr-len)
  (let ((err (cl-rdkafka/ll:rd-kafka-adminoptions-set-validate-only
              admin-options
              (if validatep 1 0)
              errstr
              errstr-len)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error "~&Failed to set validate-only to true/false: ~S"
             (cffi:foreign-string-to-lisp errstr :max-chars (1- errstr-len))))))


(defun make-queue (rd-kafka-client)
  (let ((queue (cl-rdkafka/ll:rd-kafka-queue-new rd-kafka-client)))
    (when (cffi:null-pointer-p queue)
      (error "~&Failed to create new queue"))
    queue))


(defmacro event->result (event result)
  (let ((res (gensym))
        (function (find-symbol
                   (format nil "RD-KAFKA-EVENT-~A-RESULT" result)
                   'cl-rdkafka/ll)))
    (unless function
      (error "~&Could not find function for: ~S" result))
    `(let ((,res (,function ,event)))
       (when (cffi:null-pointer-p ,res)
         (error "~&Unexpected result type, expected: ~S" ',result))
       ,res)))


(defmacro assert-successful-event (event result)
  (let ((function (find-symbol
                   (format nil "RD-KAFKA-~A-RESULT-TOPICS" result)
                   'cl-rdkafka/ll))
        (count (gensym)))
    (unless function
      (error "~&Could not find function for: ~S" result))
    `(cffi:with-foreign-object (,count :pointer)
       (loop
          with results = (,function (event->result ,event ,result) ,count)
          with count = (cffi:mem-ref ,count 'cl-rdkafka/ll:size-t)

          for i below count
          for *results = (cffi:mem-aref results :pointer i)

          for err = (cl-rdkafka/ll:rd-kafka-topic-result-error *results)
          for errstr = (cl-rdkafka/ll:rd-kafka-topic-result-error-string *results)
          for topic = (cl-rdkafka/ll:rd-kafka-topic-result-name *results)

          unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
          do (error "~&Failed to perform ~S on topic ~S: ~S" ',result topic errstr)))))


(defmacro perform-admin-op (op rd-kafka-client admin-options admin-object)
  (let ((function (find-symbol
                   (format nil "RD-KAFKA-~A" op)
                   'cl-rdkafka/ll))
        (event (gensym))
        (array (gensym))
        (queue (gensym)))
    (unless function
      (error "~&Could not find function for: ~S" op))
    `(cffi:with-foreign-object (,array :pointer 1)
       (let (,queue ,event)
         (unwind-protect
              (progn
                (setf (cffi:mem-aref ,array :pointer 0) ,admin-object
                      ,queue (make-queue ,rd-kafka-client))
                (,function ,rd-kafka-client ,array 1 ,admin-options ,queue)
                (setf ,event (cl-rdkafka/ll:rd-kafka-queue-poll ,queue 2000))
                (when (cffi:null-pointer-p ,event)
                  (error "~&Failed to get event from queue"))
                (assert-successful-event ,event ,op))
           (when ,event
             (cl-rdkafka/ll:rd-kafka-event-destroy ,event))
           (when ,queue
             (cl-rdkafka/ll:rd-kafka-queue-destroy ,queue)))))))
