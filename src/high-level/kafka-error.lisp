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

(defclass kafka-error ()
  ((rd-kafka-resp-err
    :initarg :rd-kafka-resp-err
    :initform (error "Must supply rd-kafka-resp-err pointer.")
    :documentation "rd_kafka_resp_err_t enum.")
   (code
    :reader error-code
    :documentation "Error code.")
   (description
    :reader error-description
    :documentation "Error description.")))

(defmethod initialize-instance :after ((kafka-error kafka-error) &key)
  (with-slots (rd-kafka-resp-err code description) kafka-error
    (setf code (cl-rdkafka/ll:num rd-kafka-resp-err)
          description (cl-rdkafka/ll:rd-kafka-err2str rd-kafka-resp-err))))
