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

(defpackage #:test/high-level/headers
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/headers)

(test headers
  (with-topics ((topic "headers-topic"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)
                     :serde (lambda (string)
                              (babel:string-to-octets string :encoding :utf-8))))
          (consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "headers-group-id"
                                 "auto.offset.reset" "earliest"
                                 "enable.partition.eof" "false")
                     :serde (lambda (bytes)
                              (babel:octets-to-string bytes :encoding :utf-8))))
          (expected-message "some clever message")
          (expected-headers '(("snarky header 1" . #(2 4 6))
                              ("cheeky header 2" . #(8 10 12))
                              ("creative header 3" . #(14 16 18)))))
      (kf:subscribe consumer (list topic))

      (kf:produce producer topic expected-message :headers expected-headers)
      (kf:flush producer 5000)

      (let ((message (kf:poll consumer 5000)))
        (is (string= expected-message (kf:value message)))
        (map nil
             (lambda (expected actual)
               (is (string= (car expected) (car actual)))
               (is (equalp (cdr expected) (cdr actual))))
             expected-headers
             (kf:headers message))))))
