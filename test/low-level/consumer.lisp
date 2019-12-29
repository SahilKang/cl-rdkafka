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

(in-package #:cl-user)

(defpackage #:test/low-level/consumer
  (:use #:cl #:1am #:test))

(in-package #:test/low-level/consumer)

(defun set-conf (conf key value errstr errstr-len)
  (let ((result (cl-rdkafka/ll:rd-kafka-conf-set
                 conf
                 key
                 value
                 errstr
                 errstr-len)))
    (unless (eq result 'cl-rdkafka/ll:rd-kafka-conf-ok)
      (error "Failed to set conf name `~A` to `~A`: `~A`"
             key
             value
             (cffi:foreign-string-to-lisp errstr :max-chars errstr-len)))))

(defun make-conf (alist errstr errstr-len)
  (let ((conf (cl-rdkafka/ll:rd-kafka-conf-new)))
    (when (cffi:null-pointer-p conf)
      (error "Failed to allocate conf"))
    (handler-case
        (flet ((set-conf (kv-cons)
                 (set-conf conf (car kv-cons) (cdr kv-cons) errstr errstr-len)))
          (map nil #'set-conf alist)
          conf)
      (condition (c)
        (cl-rdkafka/ll:rd-kafka-conf-destroy conf)
        (error c)))))


(defun make-consumer (conf errstr errstr-len)
  (let ((consumer (cl-rdkafka/ll:rd-kafka-new
                   cl-rdkafka/ll:rd-kafka-consumer
                   conf
                   errstr
                   errstr-len)))
    (when (cffi:null-pointer-p consumer)
      (error "Failed to allocate consumer: `~A`"
             (cffi:foreign-string-to-lisp errstr :max-chars errstr-len)))
    consumer))

(defun make-toppar-list (topics)
  (let ((toppar-list (cl-rdkafka/ll:rd-kafka-topic-partition-list-new
                      (length topics))))
    (when (cffi:null-pointer-p toppar-list)
      (error "Failed to allocate toppar-list"))
    (handler-case
        (flet ((add-topic (topic)
                 (cl-rdkafka/ll:rd-kafka-topic-partition-list-add
                  toppar-list topic -1)))
          (map nil #'add-topic topics)
          toppar-list)
      (condition (c)
        (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy toppar-list)
        (error c)))))

(defun subscribe (consumer topics)
  (let ((toppar-list (make-toppar-list topics)))
    (unwind-protect
         (let ((err (cl-rdkafka/ll:rd-kafka-subscribe consumer toppar-list)))
           (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error "Failed to subscribe to topics `~A`: `~A`"
                    topics
                    (cl-rdkafka/ll:rd-kafka-err2str err))))
      (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy toppar-list))))

(defun parse-message (message)
  (let ((payload (getf message 'cl-rdkafka/ll:payload))
        (len (getf message 'cl-rdkafka/ll:len))
        (err (getf message 'cl-rdkafka/ll:err)))
    (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error "Bad message: `~A`" (cl-rdkafka/ll:rd-kafka-err2str err)))
    (cffi:foreign-string-to-lisp payload :max-chars len)))

(defun consume (consumer)
  (let ((message (cl-rdkafka/ll:rd-kafka-consumer-poll consumer 5000)))
    (unless (cffi:null-pointer-p message)
      (unwind-protect
           (parse-message
            (cffi:mem-ref message '(:struct cl-rdkafka/ll:rd-kafka-message)))
        (cl-rdkafka/ll:rd-kafka-message-destroy message)))))

(defun commit (consumer)
  (let ((err (cl-rdkafka/ll:rd-kafka-commit consumer (cffi:null-pointer) 0)))
    (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (error "Commit error: `~A`" (cl-rdkafka/ll:rd-kafka-err2str err)))))


(defun produce-messages (bootstrap-servers topic messages)
  (uiop:run-program
   (format nil "echo -n '~A' | kafkacat -P -D '|' -b '~A' -t '~A'"
           (reduce (lambda (agg s) (format nil "~A|~A" agg s)) messages)
           bootstrap-servers
           topic)
   :force-shell t))


(test consumer
  (with-topics ((topic "consumer-test-topic"))
    (let ((group-id "consumer-group-id")
          (expected '("Hello" "World" "!"))
          (errstr-len 512)
          conf
          consumer)
      (produce-messages *bootstrap-servers* topic expected)
      (sleep 2)
      (unwind-protect
           (cffi:with-foreign-object (errstr :char errstr-len)
             (setf conf (make-conf `(("bootstrap.servers" . ,*bootstrap-servers*)
                                     ("group.id" . ,group-id)
                                     ("enable.auto.commit" . "false")
                                     ("auto.offset.reset" . "earliest")
                                     ("offset.store.method" . "broker")
                                     ("enable.partition.eof" . "false"))
                                   errstr
                                   errstr-len)
                   consumer (make-consumer conf errstr errstr-len))
             ;; set conf to nil because consumer was successfully
             ;; allocated and it takes ownership of conf pointer
             (setf conf nil)
             (subscribe consumer (list topic))
             (is (equal expected
                        (loop
                           repeat (length expected)
                           collect (consume consumer)
                           do (commit consumer)))))
        (when conf
          (cl-rdkafka/ll:rd-kafka-conf-destroy conf))
        (when consumer
          (cl-rdkafka/ll:rd-kafka-consumer-close consumer)
          (cl-rdkafka/ll:rd-kafka-destroy consumer))))))
