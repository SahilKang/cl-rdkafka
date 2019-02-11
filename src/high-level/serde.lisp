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

(in-package #:cl-rdkafka)

(defgeneric object->bytes (object &key &allow-other-keys))
(defgeneric bytes->object (bytes object-type &key &allow-other-keys))

(defmethod object->bytes
    ((object string)
     &key (encoding :utf-8)
       &allow-other-keys)
  (babel:string-to-octets object :encoding encoding))

(defmethod bytes->object
    ((bytes vector)
     (object-type (eql 'string))
     &key (encoding :utf-8)
       &allow-other-keys)
  (babel:octets-to-string bytes :encoding encoding))
