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

(defmacro make-non-interned-keyword (atom)
  `(let ((string (concatenate 'string "#:" (string ,atom))))
     (read-from-string string)))

(defmacro parse-to-export (sexp)
  (let ((s (gensym)))
    `(let ((,s ,sexp))
       (when (listp ,s)
         (cond
           ((eq (first ,s) 'constant)
            (list (make-non-interned-keyword (caadr ,s))))

           ((find (first ,s) '(defctype ctype) :test #'eq)
            (list (make-non-interned-keyword (second ,s))))

           ((find (first ,s) '(defcenum defcstruct) :test #'eq)
            (loop
               for x in (rest ,s)
               for y = (if (listp x) (first x) x)
               collect (make-non-interned-keyword y)))

           ((eq (first ,s) 'defcfun)
            (let ((snake (substitute #\- #\_ (string (second ,s)))))
              (list (make-non-interned-keyword snake)))))))))

(defmacro exports-from-file (filename)
  `(with-open-file (stream ,filename)
     (loop
        for sexp = (read stream nil 'eof)
        until (eq sexp 'eof)
        when (parse-to-export sexp)
        nconc it)))

(defmacro get-exports ()
  `(let ((exports
          (loop
             with project-root = (asdf:system-source-directory :cl-rdkafka)
             with glob-pattern = "src/low-level/librdkafka*.lisp"
             with files = (directory
                           (merge-pathnames glob-pattern project-root))

             for file in files
             append (exports-from-file file))))
     (append '(:export) exports)))

(macrolet
    ((create-package ()
       `(defpackage #:cl-rdkafka/low-level
          (:nicknames #:cl-rdkafka/ll)
          (:use #:cl #:cffi)
          ,(get-exports))))
  (create-package))
