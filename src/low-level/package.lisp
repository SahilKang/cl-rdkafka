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

;; librdkafka-bindings.lisp will export its own symbols, so let's just
;; export the librdkafka-grovel.lisp symbols here
(macrolet
    ((create-package ()
       (with-open-file (stream (merge-pathnames
                                "src/low-level/librdkafka-grovel.lisp"
                                (asdf:system-source-directory 'cl-rdkafka)))
         (labels ((->export-symbol (symbol)
                    (read-from-string (format nil "#:~A" symbol)))
                  (get-export (sexp)
                    (when (listp sexp)
                      (case (first sexp)
                        (constant (->export-symbol (caadr sexp)))
                        (ctype (->export-symbol (second sexp)))))))
           (let ((grovel-exports (loop
                                    for sexp = (read stream nil nil)
                                    while sexp
                                    when (get-export sexp)
                                    collect it)))
             `(defpackage #:cl-rdkafka/low-level
                (:nicknames #:cl-rdkafka/ll)
                (:use #:cl)
                (:export ,@grovel-exports)))))))
  (create-package))
