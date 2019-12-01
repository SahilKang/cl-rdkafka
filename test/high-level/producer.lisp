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
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/producer)

(defun parse-kafkacat (output-lines)
  (flet ((parse (partition-key-value)
           (cdr (uiop:split-string partition-key-value :separator "|"))))
    (loop
       for x in output-lines
       by #'cddr
       collect (parse x))))

(test producer-produce
  (with-topics ((topic "test-producer-produce"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)
                     :serde (lambda (x)
                              (babel:string-to-octets x :encoding :utf-8))))
          (expected '(("key-1" "Hello") ("key-2" "World") ("key-3" "!"))))
      (loop
         for (k v) in expected
         do (kf:produce producer topic v :key k)) ; TODO test partition here, too

      (kf:flush producer 5000)
      (sleep 2)

      (let* ((kafkacat-output-lines
              (uiop:run-program
               (format nil "kafkacat -CeO -K '%p|%k|%s~A' -b '~A' -t '~A'"
                       #\newline
                       *bootstrap-servers*
                       topic)
               :force-shell t
               :output :lines))
             (actual (parse-kafkacat kafkacat-output-lines)))
        (is (equal expected actual))))))

(test producer-promises
  (with-topics ((topic "test-producer-promises"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)
                     :serde #'babel:string-to-octets))
          (expected '(("key-1" "Hello") ("key-2" "World") ("key-3" "!"))))
      (let (actual)
        (bb:chain expected
          (:then (messages)
                 (mapcar (lambda (pair)
                           (destructuring-bind (key value) pair
                             (kf:produce producer topic value :key key)))
                         messages))
          (:then (promises)
                 (kf:flush producer 5000)
                 promises)
          (:map (message)
                (list (kf:key message) (kf:value message)))
          (:then (messages)
                 (setf actual messages))
          (:catch (c)
            (setf actual c)))
        (when (typep actual 'condition)
          (error actual))
        (is (equal expected actual))))))
