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

(in-package #:test/high-level/produce->consume)

(defvar +topic+ "test-produce-to-consume")

(defun produce-messages ()
  (let ((messages '(("key-1" "Hello") ("key-2" "World") ("key-3" "!")))
	(producer (make-instance 'kf:producer
				 :conf (kf:conf
					"bootstrap.servers" "kafka:9092")
				 :key-serde #'kf:object->bytes
				 :value-serde #'kf:object->bytes)))
    (loop
       for (k v) in messages
       do (kf:produce producer +topic+ v :key k))

    (kf:flush producer (* 2 1000))
    messages))

(defun consume-messages ()
  (let* ((serde (lambda (x) (kf:bytes->object x 'string)))
	 (conf (kf:conf
		"bootstrap.servers" "kafka:9092"
		"group.id" (write-to-string (get-universal-time))
		"enable.auto.commit" "false"
		"auto.offset.reset" "earliest"
		"offset.store.method" "broker"
		"enable.partition.eof" "false"))
	 (consumer (make-instance 'kf:consumer
				  :conf conf
				  :key-serde serde
				  :value-serde serde)))
    (kf:subscribe consumer (list +topic+))

    (loop
       for message = (kf:poll consumer (* 2 1000))
       while message

       for key = (kf:key message)
       for value = (kf:value message)

       collect (list key value)

       do (kf:value (kf:commit consumer)))))

(def-test produce->consume ()
  (let ((expected (produce-messages))
	(actual (consume-messages))
	(same-pair-p (lambda (lhs rhs)
		       (and
			(= 2 (length lhs) (length rhs))
			(every #'string= lhs rhs)))))
    (is
     (and
      (= (length expected) (length actual))
      (every same-pair-p expected actual)))))
