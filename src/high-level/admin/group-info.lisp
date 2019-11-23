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

(defgeneric group-info
    (client group &key timeout-ms)
  (:documentation
   "Return group info about GROUP as a list of alists.

The second boolean return value indicates if some brokers failed to
respond in time.

GROUP can be either:
  * a string indicating which group to return info for.
  * nil, in which case info for all groups is returned.

Each alist looks something like:
((:GROUP . \"clever-group-name-to-showcase-my-creative-personality\")
 (:BROKER (:ID . 1001)
          (:HOST . \"127.0.0.1\")
          (:PORT . 9092))
 (:STATE . \"Stable\")
 (:PROTOCOL-TYPE . \"consumer\")
 (:PROTOCOL . \"range\")
 (:MEMBERS
  ((:ID . \"rdkafka-b4af90cc-bd32-4b24-a0e6-cbad8f5888aa\")
   (:CLIENT-ID . \"rdkafka\")
   (:CLIENT-HOST . \"/172.18.0.1\")
   (:METADATA
    . #(0 0 0 0 0 1 0 15 102 111 111 45 98 97 114 45 116 111 112 105 99 45 51
        0 0 0 1 0 0 0 0 0 0 0 0))
   (:ASSIGNMENT
    . #(0 0 0 0 0 1 0 15 102 111 111 45 98 97 114 45 116 111 112 105 99 45 51
        0 0 0 1 0 0 0 0 0 0 0 0)))))"))


(defun parse-broker-group-info (group-info)
  (let* ((broker (getf group-info 'cl-rdkafka/ll:broker))
         (id (getf broker 'cl-rdkafka/ll:id))
         (host (getf broker 'cl-rdkafka/ll:host))
         (port (getf broker 'cl-rdkafka/ll:port)))
    (mapcar #'cons '(:id :host :port) (list id host port))))

(defun parse-member-info (member-info)
  (macrolet
      ((parse-byte-vector (symbol)
         (let ((count (find-symbol (format nil "MEMBER-~A-SIZE" symbol)
                                   'cl-rdkafka/ll))
               (pointer (find-symbol (format nil "MEMBER-~A" symbol)
                                     'cl-rdkafka/ll)))
           (unless (and count pointer)
             (error "~&Could not find count and pointer symbols: ~S" symbol))
           `(loop
               with count = (getf member-info ',count)
               with array = (getf member-info ',pointer)
               with vector = (make-array count
                                         :element-type '(unsigned-byte 8)
                                         :fill-pointer 0)

               for i below count
               for elt = (cffi:mem-aref array :uint8 i)
               do (vector-push elt vector)

               finally (return vector)))))
    (let ((id (getf member-info 'cl-rdkafka/ll:member-id))
          (client-id (getf member-info 'cl-rdkafka/ll:client-id))
          (client-host (getf member-info 'cl-rdkafka/ll:client-host))
          (metadata (parse-byte-vector metadata))
          (assignment (parse-byte-vector assignment)))
      (mapcar #'cons
              '(:id :client-id :client-host :metadata :assignment)
              (list id client-id client-host metadata assignment)))))

(defun parse-members-group-info (group-info)
  (loop
     with count = (getf group-info 'cl-rdkafka/ll:member-cnt)
     with members = (getf group-info 'cl-rdkafka/ll:members)

     for i below count
     for member = (cffi:mem-aref
                   members
                   '(:struct cl-rdkafka/ll:rd-kafka-group-member-info)
                   i)

     collect (parse-member-info member)))

(defun parse-group-info (group-info)
  (let ((group (getf group-info 'cl-rdkafka/ll:group))
        (broker (parse-broker-group-info group-info))
        (state (getf group-info 'cl-rdkafka/ll:state))
        (protocol-type (getf group-info 'cl-rdkafka/ll:protocol-type))
        (protocol (getf group-info 'cl-rdkafka/ll:protocol))
        (members (parse-members-group-info group-info))
        (err (getf group-info 'cl-rdkafka/ll:err)))
    (mapcar
     #'cons
     '(:err :group :broker :state :protocol-type :protocol :members)
     (list err group broker state protocol-type protocol members))))

(defun parse-group-list (group-list)
  (loop
     with count = (getf group-list 'cl-rdkafka/ll:group-cnt)
     with groups = (getf group-list 'cl-rdkafka/ll:groups)

     for i below count
     for group =  (cffi:mem-aref
                   groups
                   '(:struct cl-rdkafka/ll:rd-kafka-group-info)
                   i)
     for group-info =
       (let* ((group-info (parse-group-info group))
              (err (cdr (assoc :err group-info))))
         (if (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
             (delete :err group-info :key #'car)
             (let ((group (cdr (assoc :group group-info)))
                   (broker (cdr (assoc :broker group-info))))
               (cerror
                (format nil "Skip group `~A` from broker `~A` and continue with others."
                        group
                        broker)
                'kafka-error
                :description
                (format nil "Broker `~A` returned error for group `~A`: `~A`"
                        broker
                        group
                        (cl-rdkafka/ll:rd-kafka-err2str err))))))

     when group-info
     collect group-info))

(defun %group-info (rd-kafka-client group-pointer timeout-ms)
  (cffi:with-foreign-object (group-list :pointer)
    (let ((err (cl-rdkafka/ll:rd-kafka-list-groups
                rd-kafka-client
                group-pointer
                group-list
                timeout-ms))
          *group-list
          partial-errors-p)
      (unwind-protect
           (progn
             (setf partial-errors-p
                   (eq err cl-rdkafka/ll:rd-kafka-resp-err--partial))
             (unless (or (eq err cl-rdkafka/ll:rd-kafka-resp-err-no-error)
                         partial-errors-p)
               (error 'kafka-error
                      :description (cl-rdkafka/ll:rd-kafka-err2str err)))
             (setf *group-list (cffi:mem-ref group-list :pointer))
             (values
              (parse-group-list
               (cffi:mem-ref *group-list
                             '(:struct cl-rdkafka/ll:rd-kafka-group-list)))
              partial-errors-p))
        (when *group-list
          (cl-rdkafka/ll:rd-kafka-group-list-destroy *group-list))))))

(def-admin-methods
    group-info
    (client (group string) &key (timeout-ms 5000))
  (cffi:with-foreign-string (group group)
    (%group-info pointer group timeout-ms)))

(def-admin-methods
    group-info
    (client (group null) &key (timeout-ms 5000))
  (%group-info pointer (cffi:null-pointer) timeout-ms))
