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

(defpackage #:test/high-level/admin
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/admin)

(defun make-sed-commmand (topic)
  (format nil "sed -En 's/^\\s+topic\\s+~S\\s+with\\s+([0-9]+).*/\\1/p'" topic))

(defun make-kafkacat-command (bootstrap-servers topic)
  (let ((sed (make-sed-commmand topic)))
    (format nil "kafkacat -b '~A' -L -t '~A' | ~A" bootstrap-servers topic sed)))

(defun get-partitions (bootstrap-servers topic)
  (parse-integer
   (uiop:run-program
    (make-kafkacat-command bootstrap-servers topic)
    :force-shell t
    :output 'string)))


(test create-topic-with-consumer
  (with-topics ((topic "create-topic-with-consumer" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (partitions 3))
      (is (string= topic (kf::create-topic consumer
                                           topic
                                           :partitions partitions
                                           :timeout-ms 5000)))
      (sleep 2)
      (is (= partitions (get-partitions *bootstrap-servers* topic))))))

(test create-topic-with-producer
  (with-topics ((topic "create-topic-with-producer" t))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (partitions 7))
      (is (string= topic (kf::create-topic producer
                                           topic
                                           :partitions partitions
                                           :timeout-ms 5000)))
      (sleep 2)
      (is (= partitions (get-partitions *bootstrap-servers* topic))))))

(test create-topic-validatep
  (with-topics ((topic "create-topic-with-validate-only" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (partitions 4))
      (is (string= topic (kf::create-topic consumer
                                           topic
                                           :partitions partitions
                                           :timeout-ms 5000
                                           :validate-only-p t)))
      (sleep 2)
      (is (= 0 (get-partitions *bootstrap-servers* topic))))))


(test delete-topic-with-consumer
  (with-topics ((topic "delete-topic-with-consumer" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (partitions 7))
      (is (string= topic (kf::create-topic consumer
                                           topic
                                           :partitions partitions
                                           :timeout-ms 5000)))
      (sleep 2)
      (is (= partitions (get-partitions *bootstrap-servers* topic)))

      (is (string= topic (kf::delete-topic consumer topic :timeout-ms 5000)))
      (sleep 2)
      (is (= 0 (get-partitions *bootstrap-servers* topic))))))

(test delete-topic-with-producer
  (with-topics ((topic "delete-topic-with-producer" t))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (partitions 4))
      (is (string= topic (kf::create-topic producer
                                           topic
                                           :partitions partitions
                                           :timeout-ms 5000)))
      (sleep 2)
      (is (= partitions (get-partitions *bootstrap-servers* topic)))

      (is (string= topic (kf::delete-topic producer topic :timeout-ms 5000)))
      (sleep 2)
      (is (= 0 (get-partitions *bootstrap-servers* topic))))))


(test create-partitions-with-consumer
  (with-topics ((topic "create-partitions-with-consumer" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (old-partitions 7)
          (new-partitions 10))
      (is (string= topic (kf::create-topic consumer
                                           topic
                                           :partitions old-partitions)))
      (sleep 2)
      (is (= old-partitions (get-partitions *bootstrap-servers* topic)))

      (is (= new-partitions (kf::create-partitions consumer topic new-partitions)))
      (sleep 2)
      (is (= new-partitions (get-partitions *bootstrap-servers* topic))))))

(test create-partitions-with-producer
  (with-topics ((topic "create-partitions-with-producer" t))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (old-partitions 11)
          (new-partitions 20))
      (is (string= topic (kf::create-topic producer
                                           topic
                                           :partitions old-partitions)))
      (sleep 2)
      (is (= old-partitions (get-partitions *bootstrap-servers* topic)))

      (is (= new-partitions (kf::create-partitions producer topic new-partitions)))
      (sleep 2)
      (is (= new-partitions (get-partitions *bootstrap-servers* topic))))))


(test describe-topic-with-consumer
  (with-topics ((topic "describe-topic-with-consumer"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*))))
      (is (string= "CreateTime"
                   (cdr (assoc "message.timestamp.type"
                               (kf::describe-config consumer topic :topic)
                               :test #'string=)))))))

(test describe-topic-with-producer
  (with-topics ((topic "describe-topic-with-producer"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*))))
      (is (string= "CreateTime"
                   (cdr (assoc "message.timestamp.type"
                               (kf::describe-config producer topic :topic)
                               :test #'string=)))))))

(test describe-broker-with-consumer
  (destructuring-bind (host port)
      (uiop:split-string *bootstrap-servers* :separator ":")
    (let* ((consumer (make-instance
                      'kf:consumer
                      :conf (list "bootstrap.servers" *bootstrap-servers*)))
           (config (kf::describe-config consumer "1001" :broker)))
      (is (string= host
                   (cdr (assoc "advertised.host.name" config :test #'string=))))
      (is (string= port
                   (cdr (assoc "advertised.port" config :test #'string=)))))))

(test describe-broker-with-producer
  (destructuring-bind (host port)
      (uiop:split-string *bootstrap-servers* :separator ":")
    (let* ((producer (make-instance
                      'kf:producer
                      :conf (list "bootstrap.servers" *bootstrap-servers*)))
           (config (kf::describe-config producer "1001" :broker)))
      (is (string= host
                   (cdr (assoc "advertised.host.name" config :test #'string=))))
      (is (string= port
                   (cdr (assoc "advertised.port" config :test #'string=)))))))


(test alter-topic-with-consumer
  (with-topics ((topic "alter-topic-with-consumer"))
    (let* ((consumer (make-instance
                      'kf:consumer
                      :conf (list "bootstrap.servers" *bootstrap-servers*)))
           (get-actual (lambda ()
                         (cdr (assoc "message.timestamp.type"
                                     (kf::describe-config consumer topic :topic)
                                     :test #'string=)))))
      (is (string= "CreateTime" (funcall get-actual)))

      (kf::alter-config consumer
                        topic
                        '(("message.timestamp.type" . "LogAppendTime")))
      (sleep 2)
      (is (string= "LogAppendTime" (funcall get-actual))))))

(test alter-topic-with-producer
  (with-topics ((topic "alter-topic-with-producer"))
    (let* ((producer (make-instance
                      'kf:producer
                      :conf (list "bootstrap.servers" *bootstrap-servers*)))
           (get-actual (lambda ()
                         (cdr (assoc "message.timestamp.type"
                                     (kf::describe-config producer topic :topic)
                                     :test #'string=)))))
      (is (string= "CreateTime" (funcall get-actual)))

      (kf::alter-config producer
                        topic
                        '(("message.timestamp.type" . "LogAppendTime")))
      (sleep 2)
      (is (string= "LogAppendTime" (funcall get-actual))))))


(test cluster-metadata-with-consumer
  (with-topics ((topic "cluster-metadata-with-consumer"))
    (destructuring-bind (host port)
        (uiop:split-string *bootstrap-servers* :separator ":")
      (let ((consumer (make-instance
                       'kf:consumer
                       :conf (list "bootstrap.servers" *bootstrap-servers*)))
            (broker-name (format nil "~A/1001" *bootstrap-servers*)))
        (is (equal `((:originating-broker . ((:id . 1001)
                                             (:name . ,broker-name)))
                     (:broker-metadata . (((:id . 1001)
                                           (:host . ,host)
                                           (:port . ,(parse-integer port)))))
                     (:topic-metadata . (((:topic . ,topic)
                                          (:partitions . (((:id . 0)
                                                           (:leader . 1001)
                                                           (:replicas . (1001))
                                                           (:in-sync-replicas . (1001)))))))))
                   (kf::cluster-metadata consumer topic)))))))

(test cluster-metadata-with-producer
  (with-topics ((topic "cluster-metadata-with-producer"))
    (destructuring-bind (host port)
        (uiop:split-string *bootstrap-servers* :separator ":")
      (let ((producer (make-instance
                       'kf:producer
                       :conf (list "bootstrap.servers" *bootstrap-servers*)))
            (broker-name (format nil "~A/1001" *bootstrap-servers*)))
        (is (equal `((:originating-broker . ((:id . 1001)
                                             (:name . ,broker-name)))
                     (:broker-metadata . (((:id . 1001)
                                           (:host . ,host)
                                           (:port . ,(parse-integer port)))))
                     (:topic-metadata . (((:topic . ,topic)
                                          (:partitions . (((:id . 0)
                                                           (:leader . 1001)
                                                           (:replicas . (1001))
                                                           (:in-sync-replicas . (1001)))))))))
                   (kf::cluster-metadata producer topic)))))))


(test group-info-with-consumer
  (with-topics ((topic "group-info-with-consumer"))
    (let* ((group-1 "group-info-with-consumer-group-1")
           (group-2 "group-info-with-consumer-group-2")
           (consumer-1 (make-instance
                        'kf:consumer
                        :conf (list "bootstrap.servers" *bootstrap-servers*
                                    "group.id" group-1)))
           (consumer-2 (make-instance
                        'kf:consumer
                        :conf (list "bootstrap.servers" *bootstrap-servers*
                                    "group.id" group-2))))
      (kf:subscribe consumer-1 (list topic))
      (kf:subscribe consumer-2 (list topic))
      (sleep 5)

      (let ((group-info (first (kf::group-info consumer-1 group-1))))
        (is (string= group-1 (cdr (assoc :group group-info)))))

      (let ((group-info (kf::group-info consumer-1 nil)))
        (is (< 1 (length group-info)))
        (is (find group-1
                  group-info
                  :test #'string=
                  :key (lambda (alist)
                         (cdr (assoc :group alist)))))
        (is (find group-2
                  group-info
                  :test #'string=
                  :key (lambda (alist)
                         (cdr (assoc :group alist)))))))))

(test group-info-with-producer
  (with-topics ((topic "group-info-with-producer"))
    (let* ((group-1 "group-info-with-producer-group-1")
           (group-2 "group-info-with-producer-group-2")
           (consumer-1 (make-instance
                        'kf:consumer
                        :conf (list "bootstrap.servers" *bootstrap-servers*
                                    "group.id" group-1)))
           (consumer-2 (make-instance
                        'kf:consumer
                        :conf (list "bootstrap.servers" *bootstrap-servers*
                                    "group.id" group-2)))
           (producer (make-instance
                      'kf:producer
                      :conf (list "bootstrap.servers" *bootstrap-servers*))))
      (kf:subscribe consumer-1 (list topic))
      (kf:subscribe consumer-2 (list topic))
      (sleep 5)

      (let ((group-info (first (kf::group-info producer group-1))))
        (is (string= group-1 (cdr (assoc :group group-info)))))

      (let ((group-info (kf::group-info producer nil)))
        (is (< 1 (length group-info)))
        (is (find group-1
                  group-info
                  :test #'string=
                  :key (lambda (alist)
                         (cdr (assoc :group alist)))))
        (is (find group-2
                  group-info
                  :test #'string=
                  :key (lambda (alist)
                         (cdr (assoc :group alist)))))))))


(test cluster-id
  (let* ((conf (list "bootstrap.servers" *bootstrap-servers*))
         (consumer (make-instance 'kf:consumer :conf conf))
         (producer (make-instance 'kf:producer :conf conf))
         (lhs (kf::cluster-id consumer))
         (rhs (kf::cluster-id producer)))
    (is (string= lhs rhs))
    (is (not (zerop (length lhs))))))


(test controller-id
  (let* ((conf (list "bootstrap.servers" *bootstrap-servers*))
         (consumer (make-instance 'kf:consumer :conf conf))
         (producer (make-instance 'kf:producer :conf conf))
         (lhs (kf::controller-id consumer))
         (rhs (kf::controller-id producer)))
    (is (= lhs rhs))))
