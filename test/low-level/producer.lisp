;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:test/low-level/producer
  (:use #:cl #:1am #:test))

(in-package #:test/low-level/producer)

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


(defun make-producer (conf errstr errstr-len)
  (let ((producer (cl-rdkafka/ll:rd-kafka-new
                   cl-rdkafka/ll:rd-kafka-producer
                   conf
                   errstr
                   errstr-len)))
    (when (cffi:null-pointer-p producer)
      (error "Failed to allocate producer: `~A`"
             (cffi:foreign-string-to-lisp errstr :max-chars errstr-len)))
    producer))

(defun produce (producer topic message)
  (unwind-protect
       (cffi:with-foreign-string (buf message)
         (let ((err (cl-rdkafka/ll:rd-kafka-producev
                     producer

                     :int cl-rdkafka/ll:rd-kafka-vtype-topic
                     :string topic

                     :int cl-rdkafka/ll:rd-kafka-vtype-value
                     :pointer buf
                     cl-rdkafka/ll:size-t (length message)

                     :int cl-rdkafka/ll:rd-kafka-vtype-msgflags
                     :int cl-rdkafka/ll:rd-kafka-msg-f-copy

                     :int cl-rdkafka/ll:rd-kafka-vtype-end)))
           (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (error "Failed to produce message: `~A`"
                    (cl-rdkafka/ll:rd-kafka-err2str err)))))
    (cl-rdkafka/ll:rd-kafka-poll producer 0)))

(defun flush (producer)
  (let ((err (cl-rdkafka/ll:rd-kafka-flush producer 5000)))
    (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (if (eq err 'cl-rdkafka/ll:rd-kafka-resp-err--timed-out)
          (error "Flush timed out")
          (error "Flush error: `~A`" (cl-rdkafka/ll:rd-kafka-err2str err))))))


(defun consume-messages (bootstrap-servers topic)
  (uiop:run-program
   (format nil "kafkacat -Ce -b '~A' -t '~A'" bootstrap-servers topic)
   :force-shell t
   :output :lines))


(test producer
  (with-topics ((topic "producer-test-topic"))
    (let ((expected '("Hello" "World" "!"))
          (errstr-len 512)
          conf
          producer)
      (unwind-protect
           (cffi:with-foreign-object (errstr :char errstr-len)
             (setf conf (make-conf `(("bootstrap.servers" . ,*bootstrap-servers*))
                                   errstr
                                   errstr-len)
                   producer (make-producer conf errstr errstr-len))
             ;; set conf to nil because producer was successfully
             ;; allocated and it takes ownership of conf pointer
             (setf conf nil)
             (map nil
                  (lambda (message)
                    (produce producer topic message))
                  expected)
             (flush producer)
             (is (equal expected
                        (consume-messages *bootstrap-servers* topic))))
        (when conf
          (cl-rdkafka/ll:rd-kafka-conf-destroy conf))
        (when producer
          (cl-rdkafka/ll:rd-kafka-destroy producer))))))
