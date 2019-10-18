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

(defpackage #:test/high-level/producer
  (:use #:cl #:1am))

(in-package #:test/high-level/producer)

(defvar *conf* (kf:conf
                "bootstrap.servers" "kafka:9092"))

(defun parse-kafkacat (output-lines)
  (flet ((parse (partition-key-value)
           (cdr (uiop:split-string partition-key-value :separator "|"))))
    (loop
       for x in output-lines
       by #'cddr
       collect (parse x))))

(defun same-pairs-p (lhs rhs)
  (flet ((same-pair-p (lhs rhs)
           (and
            (= 2 (length lhs) (length rhs))
            (every #'string= lhs rhs))))
    (and
     (= (length lhs) (length rhs))
     (every #'same-pair-p lhs rhs))))

(test producer-produce
  (let* ((serde (lambda (x) (babel:string-to-octets x :encoding :utf-8)))
         (bootstrap-servers (gethash "bootstrap.servers" *conf*))
         (topic "test-producer-produce")
         (expected '(("key-1" "Hello") ("key-2" "World") ("key-3" "!")))
         (producer (make-instance 'kf:producer
                                  :conf *conf*
                                  :key-serde serde
                                  :value-serde serde)))
    (loop
       for (k v) in expected
       do (kf:produce producer topic v :key k)) ; TODO test partition here, too

    (kf:flush producer (* 2 1000))

    (let* ((kafkacat-output-lines
            (uiop:run-program
             (format nil "kafkacat -CeO -K '%p|%k|%s~A' -b '~A' -t '~A'"
                     #\newline
                     bootstrap-servers
                     topic)
             :force-shell t
             :output :lines
             :error-output t
             :ignore-error-status t))
           (actual (parse-kafkacat kafkacat-output-lines)))
      (is (same-pairs-p expected actual)))))
