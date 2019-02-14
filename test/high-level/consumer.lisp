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

(in-package #:test/high-level/consumer)

(defvar *conf* (make-hash-table :test #'equal))

(setf (gethash "bootstrap.servers" *conf*) "kafka:9092")
(setf (gethash "group.id" *conf*) (write-to-string (get-universal-time)))
(setf (gethash "enable.auto.commit" *conf*) "true")
(setf (gethash "auto.offset.reset" *conf*) "earliest")
(setf (gethash "offset.store.method" *conf*) "broker")
(setf (gethash "enable.partition.eof" *conf*) "true")

(def-test consumer-subscribe ()
  (setf (gethash "group.id" *conf*) (write-to-string (get-universal-time)))
  (let ((consumer (make-instance 'kf:consumer :conf *conf*))
	(expected '("consumer-test-topic" "foobar"))
	actual)
    (kf:subscribe consumer expected)
    (setf actual (sort (kf:subscription consumer) #'string<))
    (kf:unsubscribe consumer)
    (is (and
	 (= (length expected) (length actual))
	 (every #'string= expected actual)
	 (= 0 (length (kf:subscription consumer)))))))

(def-test poll ()
  (setf (gethash "group.id" *conf*) (write-to-string (get-universal-time)))
  (let ((consumer (make-instance 'kf:consumer
				 :conf *conf*
				 :value-serde (lambda (x)
						(kf:bytes->object x 'string))))
	(expected '("Hello" "World" "!"))
	(actual (make-array 3 :element-type 'string :initial-element "")))
    (kf:subscribe consumer '("consumer-test-topic"))

    (loop
       for i below 3
       for message = (kf:poll consumer 5000)
       for value = (kf:value message)
       do (setf (elt actual i) value))

    (is (and
	 (= (length expected) (length actual))
	 (every #'string= expected actual)))))
