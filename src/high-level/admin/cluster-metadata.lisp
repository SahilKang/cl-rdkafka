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

(defgeneric cluster-metadata
    (client topic &key timeout-ms)
  (:documentation
   "Return an alist of cluster metadata.

TOPIC can be one of:
  * a string indicating the topic name to return metadata for.
  * :LOCAL, in which case metadata for only locally known topics is returned.
  * :ALL, in which case metadata for all cluster topics is returned.

The returned alist looks something like:
((:originating-broker . ((:id . 1001)
                         (:name . \"127.0.0.1:9092/1001\")))
 (:broker-metadata . (((:id . 1001)
                       (:host . \"127.0.0.1\")
                       (:port . 9092))))
 (:topic-metadata . (((:topic . \"topic-name\")
                      (:partitions . (((:id . 0)
                                       (:err . nil)
                                       (:leader . 1001)
                                       (:replicas . (1001))
                                       (:in-sync-replicas . (1001)))))
                      (:err . nil)))))"))

;; TODO think about switching timeout-ms from a keyword arg to an
;; optional arg for these admin methods
(defgeneric cluster-id
    (client &key timeout-ms)
  (:documentation
   "Returns the cluster-id as reported in the broker metadata."))

(defgeneric controller-id
    (client &key timeout-ms)
  (:documentation
   "Returns the controller-id as reported in the broker metadata, or nil."))


(defun parse-broker-metadata (metadata)
  "Return a list of (:id :host :port) alists."
  (loop
     with count = (getf metadata 'cl-rdkafka/ll:broker-cnt)
     with brokers = (getf metadata 'cl-rdkafka/ll:brokers)

     for i below count
     for broker = (cffi:mem-aref
                   brokers
                   '(:struct cl-rdkafka/ll:rd-kafka-metadata-broker)
                   i)

     for id = (getf broker 'cl-rdkafka/ll:id)
     for host = (getf broker 'cl-rdkafka/ll:host)
     for port = (getf broker 'cl-rdkafka/ll:port)

     collect (mapcar #'cons '(:id :host :port) (list id host port))))

(defun parse-partitions-metadata (topic-metadata)
  "Return a list of (:id :err :leader :replicas :in-sync-replicas) alists."
  (flet ((parse-list (struct array-symbol count-symbol)
           (loop
              with count = (getf struct count-symbol)
              with array = (getf struct array-symbol)
              for i below count
              for elt = (cffi:mem-aref array :int32 i)
              collect elt)))
    (loop
       with count = (getf topic-metadata 'cl-rdkafka/ll:partition-cnt)
       with partitions = (getf topic-metadata 'cl-rdkafka/ll:partitions)

       for i below count
       for partition = (cffi:mem-aref
                        partitions
                        '(:struct cl-rdkafka/ll:rd-kafka-metadata-partition)
                        i)

       for id = (getf partition 'cl-rdkafka/ll:id)
       ;; TODO should signal an error instead of keeping it as a field
       for err = (let ((err (getf partition 'cl-rdkafka/ll:err)))
                   (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
                     (cl-rdkafka/ll:rd-kafka-err2str err)))
       for leader = (getf partition 'cl-rdkafka/ll:leader)
       for replicas = (parse-list partition
                                  'cl-rdkafka/ll:replicas
                                  'cl-rdkafka/ll:replica-cnt)
       for in-sync-replicas = (parse-list partition
                                          'cl-rdkafka/ll:isrs
                                          'cl-rdkafka/ll:isr-cnt)

       collect (mapcar #'cons
                       '(:id :err :leader :replicas :in-sync-replicas)
                       (list id err leader replicas in-sync-replicas)))))

(defun parse-topic-metadata (metadata)
  "Return a list of (:topic :partitions :err) alists."
  (loop
     with count = (getf metadata 'cl-rdkafka/ll:topic-cnt)
     with topics = (getf metadata 'cl-rdkafka/ll:topics)

     for i below count
     for topic = (cffi:mem-aref
                  topics
                  '(:struct cl-rdkafka/ll:rd-kafka-metadata-topic)
                  i)

     for name = (getf topic 'cl-rdkafka/ll:topic)
     for partitions = (parse-partitions-metadata topic)
     ;; TODO should signal an error instead of keeping it as a field
     for err = (let ((err (getf topic 'cl-rdkafka/ll:err)))
                 (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
                   (cl-rdkafka/ll:rd-kafka-err2str err)))

     collect (mapcar #'cons '(:topic :partitions :err) (list name partitions err))))

(defun parse-cluster-metadata (metadata)
  "Returns a (:originating-broker :broker-metadata :topic-metadata) alist."
  (let ((broker-metadata (parse-broker-metadata metadata))
        (topic-metadata (parse-topic-metadata metadata))
        (originating-broker-id (getf metadata 'cl-rdkafka/ll:orig-broker-id))
        (originating-broker-name (getf metadata 'cl-rdkafka/ll:orig-broker-name)))
    (list
     (cons :originating-broker (mapcar #'cons
                                       '(:id :name)
                                       (list originating-broker-id
                                             originating-broker-name)))
     (cons :broker-metadata broker-metadata)
     (cons :topic-metadata topic-metadata))))

(defun %cluster-metadata (rd-kafka-client all-topics topic-handle timeout-ms)
  (cffi:with-foreign-object (metadata :pointer)
    (let ((err (cl-rdkafka/ll:rd-kafka-metadata
                rd-kafka-client
                all-topics
                topic-handle
                metadata
                timeout-ms)))
      (unless (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
        (error "~&Failed to get cluster metadata: ~S"
               (cl-rdkafka/ll:rd-kafka-err2str err)))
      (let ((*metadata (cffi:mem-ref metadata :pointer)))
        (unwind-protect
             (parse-cluster-metadata
              (cffi:mem-ref *metadata '(:struct cl-rdkafka/ll:rd-kafka-metadata)))
          (cl-rdkafka/ll:rd-kafka-metadata-destroy *metadata))))))

(def-admin-methods
    cluster-metadata
    (client (topic string) &key (timeout-ms 5000))
  (let (topic-handle)
    (unwind-protect
         (progn
           (setf topic-handle (make-topic pointer topic))
           (%cluster-metadata pointer 0 topic-handle timeout-ms))
      (when topic-handle
        (cl-rdkafka/ll:rd-kafka-topic-destroy topic-handle)))))

(def-admin-methods
    cluster-metadata
    (client (topic (eql :local)) &key (timeout-ms 5000))
  (%cluster-metadata pointer 0 (cffi:null-pointer) timeout-ms))

(def-admin-methods
    cluster-metadata
    (client (topic (eql :all)) &key (timeout-ms 5000))
  (%cluster-metadata pointer 1 (cffi:null-pointer) timeout-ms))


(def-admin-methods
    cluster-id
    (client &key (timeout-ms 5000))
  (cl-rdkafka/ll:rd-kafka-clusterid pointer timeout-ms))


(def-admin-methods
    controller-id
    (client &key (timeout-ms 5000))
  (let ((id (cl-rdkafka/ll:rd-kafka-controllerid pointer timeout-ms)))
    (unless (= id -1)
      id)))
