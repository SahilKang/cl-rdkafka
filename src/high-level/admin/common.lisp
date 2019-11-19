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
      (error 'allocation-error :name "admin-options"))
    admin-options))

(defun set-timeout (admin-options timeout-ms errstr errstr-len)
  (let ((err (cl-rdkafka/ll:rd-kafka-adminoptions-set-request-timeout
              admin-options
              timeout-ms
              errstr
              errstr-len)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error 'kafka-error
             :description
             (format nil "Failed to set request timeout of admin-options: `~A`"
                     (cffi:foreign-string-to-lisp
                      errstr :max-chars (1- errstr-len)))))))

(defun set-validate (admin-options validatep errstr errstr-len)
  (let ((err (cl-rdkafka/ll:rd-kafka-adminoptions-set-validate-only
              admin-options
              (if validatep 1 0)
              errstr
              errstr-len)))
    (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error 'kafka-error
             :description
             (format nil "Failed to set validate-only of admin-options: `~A`"
                     (cffi:foreign-string-to-lisp
                      errstr :max-chars (1- errstr-len)))))))


(defun make-queue (rd-kafka-client)
  (let ((queue (cl-rdkafka/ll:rd-kafka-queue-new rd-kafka-client)))
    (when (cffi:null-pointer-p queue)
      (error "~&Failed to create new queue"))
    queue))


(defmacro find-function (format-template &rest format-args)
  (let ((name (gensym))
        (function (gensym)))
    `(let* ((,name (format nil ,format-template ,@format-args))
            (,function (find-symbol ,name 'cl-rdkafka/ll)))
       (or ,function (error "~&Could not find function ~S" ,name)))))

(defmacro event->result (event result)
  `(let ((res (gensym))
         (function (find-function "RD-KAFKA-EVENT-~A-RESULT" ,result)))
     `(let ((,res (,function ,,event)))
        (when (cffi:null-pointer-p ,res)
          (error "~&Unexpected result type, expected: ~S" ',,result))
        ,res)))

(defun config-op-p (op)
  (declare (symbol op))
  (uiop:string-suffix-p (string op) "CONFIGS"))

(defmacro find-result-function (op)
  `(find-function "RD-KAFKA-~A-RESULT-~A"
                  ,op
                  (if (config-op-p ,op)
                      'resources
                      'topics)))

(defmacro find-field-function (op field)
  `(find-function "RD-KAFKA-~A-~A"
                  (if (config-op-p ,op)
                      'configresource
                      'topic-result)
                  ,field))

(defun parse-configentry (entry)
  (list
   (cons 'name (cl-rdkafka/ll:rd-kafka-configentry-name entry))
   (cons 'value (cl-rdkafka/ll:rd-kafka-configentry-value entry))
   (cons 'readonlyp (cl-rdkafka/ll:rd-kafka-configentry-is-read-only entry))
   (cons 'defaultp (cl-rdkafka/ll:rd-kafka-configentry-is-default entry))
   (cons 'sensitivep (cl-rdkafka/ll:rd-kafka-configentry-is-sensitive entry))
   (cons 'synonymp (cl-rdkafka/ll:rd-kafka-configentry-is-synonym entry))
   (cons 'synonyms (get-synonyms entry))))

(defmacro make-loop (array-generating-form &body body)
  "Expands to a loop form that iterates over the results of ARRAY-GENERATING-FORM.

BODY is appended as the last lines of the expanded loop form and a
POINTER symbol is bound to each array elem for BODY to use."
  (let ((count (first (last array-generating-form)))
        (array (gensym))
        (*count (gensym))
        (i (gensym)))
    `(cffi:with-foreign-object (,count 'cl-rdkafka/ll:size-t)
       (loop
          with ,array = ,array-generating-form
          with ,*count = (cffi:mem-ref ,count 'cl-rdkafka/ll:size-t)

          for ,i below ,*count
          for pointer = (cffi:mem-aref ,array :pointer ,i)

          ,@body))))

(defun get-synonyms (rd-kafka-configentry)
  (make-loop
      (cl-rdkafka/ll:rd-kafka-configentry-synonyms
       rd-kafka-configentry
       count)
    collect (parse-configentry pointer)))

(defun get-config-data (rd-kafka-configresource)
  (make-loop
      (cl-rdkafka/ll:rd-kafka-configresource-configs
       rd-kafka-configresource
       count)
    collect (parse-configentry pointer)))

(defmacro assert-successful-event (event result)
  (let ((function (find-result-function result))
        (get-err (find-field-function result 'error))
        (get-errstr (find-field-function result 'error-string))
        (get-name (find-field-function result 'name)))
    `(make-loop
         (,function ,(event->result event result) count)
       for err = (,get-err pointer)
       for errstr = (,get-errstr pointer)
       for name = (,get-name pointer)

       unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
       do (error "~&Failed to perform ~S on ~S: ~S" ',result name errstr)

       when (config-op-p ',result)
       collect (get-config-data pointer))))

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
                  (setf ,event nil)
                  (error "~&Failed to get event from queue"))
                (assert-successful-event ,event ,op))
           (when ,event
             (cl-rdkafka/ll:rd-kafka-event-destroy ,event))
           (when ,queue
             (cl-rdkafka/ll:rd-kafka-queue-destroy ,queue)))))))


(defmacro def-admin-methods (name (&rest lambda-list) &body body)
  "Define two methods named NAME with the first arg of LAMBDA-LIST
specialized to consumer and producer.

A POINTER symbol is bound to the rd-kafka-[consumer|producer] slot of
the first LAMBDA-LIST arg for BODY to use."
  (let ((client (first lambda-list)))
    (unless (and client (symbolp client))
      (error "~&First element of lambda-list should be a symbol: ~S" client))
    `(progn
       ,@(mapcar
          (lambda (class)
            (let ((slot (read-from-string
                         (format nil "rd-kafka-~A" class)))
                  (lambda-list (cons (list client class)
                                     (cdr lambda-list))))
              `(defmethod ,name ,lambda-list
                 (with-slots (,slot) ,client
                   (let ((pointer ,slot))
                     ,@body)))))
          '(consumer producer)))))
