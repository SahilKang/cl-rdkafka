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

(defpackage #:test/high-level/produce->consume
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/produce->consume)

(defvar +topic+ "test-produce-to-consume")

(defun produce-messages ()
  (let ((producer (make-instance
                   'kf:producer
                   :conf (list "bootstrap.servers" *bootstrap-servers*)
                   :serde (lambda (x)
                            (babel:string-to-octets x :encoding :utf-8))))
        (messages '(("key-1" "Hello") ("key-2" "World") ("key-3" "!"))))
    (loop
       for (k v) in messages
       do (kf:produce producer +topic+ v :key k))

    (kf:flush producer 5000)
    messages))

(defun consume-messages ()
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (list "bootstrap.servers" *bootstrap-servers*
                               "group.id" "consume-messages-group-id"
                               "enable.auto.commit" "false"
                               "auto.offset.reset" "earliest"
                               "offset.store.method" "broker"
                               "enable.partition.eof" "false")
                   :serde (lambda (x)
                            (babel:octets-to-string x :encoding :utf-8)))))
    (kf:subscribe consumer (list +topic+))

    (loop
       for message = (kf:poll consumer 5000)
       while message

       for key = (kf:key message)
       for value = (kf:value message)

       collect (list key value)

       do (kf:commit consumer))))

(test produce->consume
  (let ((expected (produce-messages))
        (actual (consume-messages)))
    (is (equal expected actual))))
