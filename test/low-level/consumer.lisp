;;; ===========================================================================
;;; Copyright (C) 2018 Sahil Kang <sahil.kang@asilaycomputing.com>
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
;;; ===========================================================================

(in-package #:test/low-level-consumer)

(defparameter *messages* (make-array 0
				     :element-type 'string
				     :adjustable t
				     :fill-pointer 0))

(defun consume-message (rk-message)
  (let* ((*rk-message (mem-ref rk-message '(:struct rd-kafka-message)))
	 (err (getf *rk-message 'cl-rdkafka/ll:err))
	 (len (getf *rk-message 'cl-rdkafka/ll:len))
	 (payload (getf *rk-message 'cl-rdkafka/ll:payload)))
    (when (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
      (let ((message (foreign-string-to-lisp payload :max-chars len)))
	(vector-push-extend message *messages*)))))

(defcallback rebalance-callback :void
    ((rk :pointer)
     (err rd-kafka-resp-err)
     (partitions :pointer)
     (opaque :pointer))
  (cond
    ((eq err cl-rdkafka/ll:rd-kafka-resp-err--assign-partitions)
     (rd-kafka-assign rk partitions))

    ((eq err cl-rdkafka/ll:rd-kafka-resp-err--revoke-partitions)
     (rd-kafka-assign rk (null-pointer)))

    (t
     (error (format nil "failed: ~A~%" (rd-kafka-err2str err)))
     (rd-kafka-assign rk (null-pointer)))))

(defun make-conf (group-id errstr errstr-len)
  (let ((conf (rd-kafka-conf-new)))
    (rd-kafka-conf-set conf "group.id" group-id errstr errstr-len)
    (rd-kafka-conf-set-rebalance-cb conf (callback rebalance-callback))
    (rd-kafka-conf-set conf "enable.partition.eof" "true" (null-pointer) 0)
    conf))

(defun make-topic-conf (conf errstr errstr-len)
  (let ((topic-conf (rd-kafka-topic-conf-new)))
    (rd-kafka-topic-conf-set topic-conf
			     "offset.store.method"
			     "broker"
			     errstr
			     errstr-len)
    (rd-kafka-topic-conf-set topic-conf
			     "auto.offset.reset"
			     "earliest"
			     errstr
			     errstr-len)
    (rd-kafka-conf-set-default-topic-conf conf topic-conf)))

(defun make-topic+partition-list (topics)
  (let ((topic+partitions (rd-kafka-topic-partition-list-new (length topics))))
    (loop
       for topic in topics
       do (rd-kafka-topic-partition-list-add topic+partitions topic -1))
    topic+partitions))

(defun make-consumer (conf brokers topics errstr errstr-len)
  (let* ((consumer (rd-kafka-new cl-rdkafka/ll:rd-kafka-consumer
				 conf
				 errstr
				 errstr-len))
	 (topic+partitions (make-topic+partition-list topics))
	 (*topic+partitions (mem-ref topic+partitions
				     '(:struct rd-kafka-topic-partition-list))))
    (unless consumer
      (error (format nil "Failed to create new consumer: ~A~%" errstr)))
    (rd-kafka-brokers-add consumer brokers)
    (rd-kafka-poll-set-consumer consumer)
    (rd-kafka-subscribe consumer topic+partitions)
    (list consumer topic+partitions)))

(defun consume (consumer)
  (let ((message (rd-kafka-consumer-poll consumer 5000)))
    (unless (null-pointer-p message)
      (consume-message message)
      (rd-kafka-message-destroy message))))

(defun init (group-id brokers topics)
  (let (conf topic-conf consumer&topic+partitions (errstr-len 512))
    (with-foreign-object (errstr :char errstr-len)
      (setf conf (make-conf group-id errstr errstr-len)
	    topic-conf (make-topic-conf conf errstr errstr-len)

	    consumer&topic+partitions
	    (make-consumer conf brokers topics errstr errstr-len)))
    consumer&topic+partitions))

(defun destroy (consumer topic+partitions)
  (rd-kafka-consumer-close consumer)
  (rd-kafka-topic-partition-list-destroy topic+partitions)
  (rd-kafka-destroy consumer))

(def-test consumer ()
  (destructuring-bind
	(consumer topic+partitions) (init (write-to-string (get-universal-time))
					  "kafka:9092"
					  (list "consumer-test-topic"))
    (consume consumer)
    (consume consumer)
    (consume consumer)
    (destroy consumer topic+partitions))
  (let ((expected '("Hello" "World" "!")))
    (is (and (= (length expected) (length *messages*))
	     (every #'string= expected *messages*)))))
