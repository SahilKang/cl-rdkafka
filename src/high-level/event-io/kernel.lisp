;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defparameter +address->queue-lock+ (bt:make-lock "address->queue-lock"))

(defparameter +address->queue+ (make-hash-table)
  "Maps an rd_kafka_queue_t pointer address to (function . queue).")

(defparameter +pointer-size+ (cffi:foreign-type-size :pointer)
  "C pointer size in bytes.")

(defvar +kernel+ nil)

(defvar +read-fd+ nil)

(defvar +write-fd+ nil)

(defun wait-for-fd (pollfd)
  (when (= -1 (loop
                 with errno = (cffi:foreign-symbol-pointer "errno")

                 for ret = (posix-poll pollfd 1 -1)
                 while (and (= ret -1)
                            (= (cffi:mem-ref errno :int) eintr))
                 finally (return ret)))
    (error 'kafka-error :description "posix-poll failed")))

(defun assert-good-revents (pollfd)
  (let ((revents (cffi:foreign-slot-value pollfd '(:struct pollfd) 'revents)))
    (unless (zerop (logand revents (logior pollerr pollhup pollnval)))
      (error 'kafka-error :description "posix-poll revents error"))
    (when (zerop (logand revents pollin))
      (error 'kafka-error
             :description "posix-poll returned but fd is not ready to read"))))

(defun read-rd-kafka-queue-from-fd (pollfd)
  (wait-for-fd pollfd)
  (assert-good-revents pollfd)
  (cffi:with-foreign-object (buf :pointer)
    (let* ((fd (cffi:foreign-slot-value pollfd '(:struct pollfd) 'fd))
           (bytes-read (posix-read fd buf +pointer-size+)))
      (when (= -1 bytes-read)
        (error 'kafka-error :description "posix-read failed"))
      (unless (= bytes-read +pointer-size+)
        (error 'kafka-error
               :description
               (format nil "Read ~A bytes instead of ~A"
                       bytes-read +pointer-size+))))
    (cffi:mem-ref buf :pointer)))

(defun process-events (rd-kafka-queue)
  (let* ((address (cffi:pointer-address rd-kafka-queue))
         (pair (gethash address +address->queue+)))
    ;; pair can be nil when deregister-rd-kafka-queue runs before the
    ;; next poll-loop iteration
    (when pair
      (loop
         with process-event = (car pair)
         with queue = (cdr pair)

         for event = (cl-rdkafka/ll:rd-kafka-queue-poll rd-kafka-queue 0)
         until (cffi:null-pointer-p event)
         do (unwind-protect
                 (funcall process-event event queue)
              (cl-rdkafka/ll:rd-kafka-event-destroy event))))))

(defun poll-loop ()
  (log:info "Entering Kafka poll loop")
  (unwind-protect
       (cffi:with-foreign-object (pollfd '(:struct pollfd))
         (setf (cffi:foreign-slot-value pollfd '(:struct pollfd) 'fd) +read-fd+
               (cffi:foreign-slot-value pollfd '(:struct pollfd) 'events) pollin)
         (loop
           for rd-kafka-queue = (read-rd-kafka-queue-from-fd pollfd)
           do (with-simple-restart (continue "Ignore error and continue polling")
                (bt:with-lock-held (+address->queue-lock+)
                  (process-events rd-kafka-queue)))))
    (log:info "Exiting Kafka poll loop")))

(defun assert-expected-event (rd-kafka-event expected)
  (let ((actual (cl-rdkafka/ll:rd-kafka-event-type rd-kafka-event)))
    (unless (= expected actual)
      (cond
        ((= actual cl-rdkafka/ll:rd-kafka-event-error)
         (error 'kafka-error
                :description
                (cl-rdkafka/ll:rd-kafka-event-error-string rd-kafka-event)))
        (t
         (error 'kafka-error
                :description
                (format nil  "Expected event-type `~A`, not `~A`"
                        expected actual)))))))

(defun init-fds ()
  (cffi:with-foreign-object (fds :int 2)
    (unless (zerop (posix-pipe fds))
      (error 'kafka-error :description "posix-pipe failed"))
    (setf +read-fd+ (cffi:mem-aref fds :int 0)
          +write-fd+ (cffi:mem-aref fds :int 1))))

(defun init-kernel ()
  ;; 2 threads: one for poll-loop and one for futures
  (setf +kernel+ (lparallel:make-kernel 2 :name "cl-rdkafka"))
  (let* ((lparallel:*kernel* +kernel+)
         (channel (lparallel:make-channel :fixed-capacity 1)))
    (lparallel:submit-task channel #'poll-loop)))

(defun enable-event-io (rd-kafka-queue)
  (unless +write-fd+
    (init-fds))
  (unless +kernel+
    (init-kernel))
  (cffi:with-foreign-object (queue-pointer :pointer)
    (setf (cffi:mem-ref queue-pointer :pointer) rd-kafka-queue)
    (cl-rdkafka/ll:rd-kafka-queue-io-event-enable
     rd-kafka-queue
     +write-fd+
     queue-pointer
     +pointer-size+)))

(defun register-rd-kafka-queue (rd-kafka-queue process-event)
  (handler-case
      (bt:with-lock-held (+address->queue-lock+)
        (let ((address (cffi:pointer-address rd-kafka-queue))
              (pair (cons process-event (lparallel.queue:make-queue))))
          (enable-event-io rd-kafka-queue)
          (setf (gethash address +address->queue+) pair)))
    (condition (c)
      (deregister-rd-kafka-queue rd-kafka-queue)
      (error c))))

(defun enqueue-payload (rd-kafka-queue payload)
  (let* ((address (cffi:pointer-address rd-kafka-queue))
         (queue (cdr (gethash address +address->queue+))))
    (lparallel.queue:push-queue payload queue)))

(defun deregister-rd-kafka-queue (rd-kafka-queue)
  (let ((address (cffi:pointer-address rd-kafka-queue)))
    (bt:with-lock-held (+address->queue-lock+)
      (remhash address +address->queue+))))
