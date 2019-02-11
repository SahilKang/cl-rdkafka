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

(defpackage #:cl-rdkafka/test
  (:use #:cl #:fiveam)
  (:export #:run-tests-for-shell))

(defpackage #:test/low-level/producer
  (:use #:cl #:cffi #:cl-rdkafka/low-level #:fiveam))

(defpackage #:test/low-level/consumer
  (:use #:cl #:cffi #:cl-rdkafka/low-level #:fiveam))

(in-package #:cl-rdkafka/test)

(defun run-tests-for-shell ()
  (let ((*on-error* nil)
	(*on-failure* nil))
    (if (run-all-tests)
	(uiop:quit 0)
	(uiop:quit 1))))
