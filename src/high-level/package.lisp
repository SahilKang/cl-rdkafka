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

(defpackage #:cl-rdkafka
  (:nicknames #:kf)
  (:use #:cl)
  (:shadow #:close)
  (:export

   ;; message class
   #:message
   #:key
   #:value
   #:topic
   #:partition
   #:offset
   #:timestamp
   #:headers

   ;; future class
   #:future
   #:value
   #:donep

   #:consumer #:subscribe #:unsubscribe #:subscription
   #:poll #:commit #:committed #:assignment #:assign #:member-id
   #:pause #:resume #:query-watermark-offsets #:offsets-for-times
   #:positions #:close

   #:producer #:produce #:flush

   #:kafka-error
   #:description
   #:topic+partition-error
   #:allocation-error
   #:name))
