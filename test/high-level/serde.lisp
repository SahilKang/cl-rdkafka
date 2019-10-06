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

(defpackage #:test/high-level/serde
  (:use #:cl #:1am))

(in-package #:test/high-level/serde)

(test string-utf8
  (let* ((expected "Hello World!")
         (actual (kf:bytes->object
                  (kf:object->bytes expected)
                  'string)))
    (is (string= expected actual))))

(test string-utf16
  (let* ((expected "Hello World!")
         (actual (kf:bytes->object
                  (kf:object->bytes expected :encoding :utf-16)
                  'string
                  :encoding :utf-16)))
    (is (string= expected actual))))
