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

(in-package #:cl-rdkafka/test)

(defun version->string (version-num)
  "Parse rdkafka.h version number to string.

librdkafka/rdkafka.h says that the version int should be interpreted as:
   MM.mm.rr.xx
     - MM = Major
     - mm = minor
     - rr = revision
     - xx pre-release id (0xff is the final release)
  So 0x000801ff = 0.8.1"
  (flet ((get-byte (offset)
           (let ((bits (* -1 (* 8 offset))))
             (logand #xff (ash version-num bits))))
         (format-pre-release (pre-release)
           (if (= pre-release #xff)
               ""
               (format nil ".~A" pre-release))))
    (let ((major (get-byte 3))
          (minor (get-byte 2))
          (revision (get-byte 1))
          (pre-release (get-byte 0)))
      (format nil "~A.~A.~A~A"
              major
              minor
              revision
              (format-pre-release pre-release)))))

(def-test check-version-funcs ()
  (let ((num (cl-rdkafka/ll:rd-kafka-version))
        (str (cl-rdkafka/ll:rd-kafka-version-str)))
    (is (string= (version->string num) str))))
