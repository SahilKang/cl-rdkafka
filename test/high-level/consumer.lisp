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

(def-test commit ()
  (setf (gethash "enable.auto.commit" *conf*) "false"
	(gethash "group.id" *conf*) "commit-test-group")

  (let ((consumer (make-instance 'kf:consumer :conf *conf*))
	(expected '(1 2 3))
	actual
	(commits (make-array 3)))
    (kf:subscribe consumer '("consumer-test-topic"))

    (loop
       for i below 3
       do (kf:poll consumer 5000)
       do (kf:value (kf:commit consumer))
       do (setf (elt commits i) (kf:committed consumer)))

    ;; each element of commits is a vector of one element
    (let ((flattened (map 'list (lambda (v) (elt v 0)) commits)))
      (setf actual (mapcar (lambda (t+p) (kf:offset t+p)) flattened)))

    (is (and
	 (= (length expected) (length actual) (length commits))
	 (every #'= expected actual)
	 (apply #'= (map 'list #'length commits))))))

(def-test assign ()
  (let ((consumer (make-instance 'kf:consumer :conf *conf*))
	assignment)
    (kf:assign consumer (list (make-instance 'kf:topic+partition
					     :topic "foobar"
					     :offset 7
					     :partition 35
					     :metadata "foobarbaz")))

    (setf assignment (elt (kf:assignment consumer) 0))

    (is (and
	 (string= "foobar" (kf:topic assignment))
	 (= 7 (kf:offset assignment))
	 (= 35 (kf:partition assignment))
	 (string= "foobarbaz" (kf:metadata assignment))))))
