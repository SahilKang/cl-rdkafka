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

(defpackage #:test
  (:use #:cl)
  (:export
   *bootstrap-servers*
   with-topics))

(in-package #:test)

(defparameter *bootstrap-servers* "127.0.0.1:9092"
  "Kafka bootstrap.servers that tests will use.")


;; this doesn't always work; sometimes the delete-topic call will
;; succeed, but the topic still persists on the cluster.
;; TODO Change create-topic and delete-topic to accept a sequence of
;; topics instead...that's a better api and it might fix the issue
(defmacro with-topics ((&rest bindings) &body body)
  "Evaluate BODY with topics optionally created and then deleted from BINDINGS.

Each element in the BINDINGS list looks like either:
  * (SYMBOL TOPIC NO-CREATE-P)
  * (SYMBOL TOPIC)
where SYMBOL is bound to the TOPIC string and NO-CREATE-P determines
if the topic should be created before BODY (defaults to nil when not
supplied).

Each of the topics in BINDINGS will be deleted once BODY evaluates."
  (when (zerop (length bindings))
    (error "No point in using this macro without bindings."))
  (destructuring-bind
        (let-bindings topics-to-create)
      (loop
         with let-bindings = nil
         with topics-to-create = nil

         for (symbol topic no-create-p) in bindings

         do (push (list symbol topic) let-bindings)
         unless no-create-p
         do (push symbol topics-to-create)

         finally (return (list (nreverse let-bindings)
                               (nreverse topics-to-create))))
    (let ((client (gensym))
          (client-form '(make-instance
                         'kf:consumer
                         :conf (list "bootstrap.servers" test:*bootstrap-servers*))))
      `(let ,(cons (list client client-form)
                   let-bindings)
         (unwind-protect
              (progn
                ,@(unless (zerop (length topics-to-create))
                    `((map nil
                           (lambda (topic)
                             (kf::create-topic ,client topic))
                           (list ,@topics-to-create))
                      (sleep 2)))
                ,@body)
           (progn
             (map nil
                  (lambda (topic)
                    (handler-case
                        (kf::delete-topic ,client topic)
                      (kf:kafka-error ())))
                  (list ,@(mapcar #'first let-bindings)))
             (sleep 2)))))))
