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

(defpackage #:test/high-level/topic+partition
  (:use #:cl #:1am))

(in-package #:test/high-level/topic+partition)

(defun equal? (lhs rhs)
  (and
   (= (kf:offset lhs) (kf:offset rhs))
   (= (kf:partition lhs) (kf:partition rhs))
   (string= (kf:topic lhs) (kf:topic rhs))
   (string= (kf:metadata lhs) (kf:metadata rhs))))

(test topic+partition
  (let* ((expected
          (list
           (make-instance 'kf:topic+partition :topic "topic-1")
           (make-instance 'kf:topic+partition
                          :topic "topic-2"
                          :offset 2
                          :partition 3)
           (make-instance 'kf:topic+partition
                          :topic "topic-3"
                          :offset 3
                          :partition 4
                          :metadata "Here's some metadata.")))
         (rd-kafka-list (kf::topic+partitions->rd-kafka-list expected))
         (actual (kf::rd-kafka-list->topic+partitions rd-kafka-list)))
    (cl-rdkafka/ll:rd-kafka-topic-partition-list-destroy rd-kafka-list)
    (is (and
         (= (length expected) (length actual))
         (every #'equal? expected actual)))))
