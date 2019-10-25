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
  (:use #:cl #:1am))

(in-package #:test/high-level/admin)

(defun make-sed-commmand (topic)
  (format nil "sed -En 's/^\\s+topic\\s+~S\\s+with\\s+([0-9]+).*/\\1/p'" topic))

(defun make-kafkacat-command (topic)
  (let ((sed (make-sed-commmand topic)))
    (format nil "kafkacat -b 'kafka:9092' -L -t '~A' | ~A" topic sed)))

(defun get-partitions (topic)
  (parse-integer
   (uiop:run-program
    (make-kafkacat-command topic)
    :force-shell t
    :output 'string)))


(test create-topic-with-consumer
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "create-topic-with-consumer")
        (partitions 3))
    (is (string= topic (kf:create-topic consumer
                                        topic
                                        :partitions partitions
                                        :timeout-ms 5000)))
    (sleep 2)
    (is (= partitions (get-partitions topic)))))

(test create-topic-with-producer
  (let ((producer (make-instance
                   'kf:producer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "create-topic-with-producer")
        (partitions 7))
    (is (string= topic (kf:create-topic producer
                                        topic
                                        :partitions partitions
                                        :timeout-ms 5000)))
    (sleep 2)
    (is (= partitions (get-partitions topic)))))

(test create-topic-validatep
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "create-topic-with-validate-only")
        (partitions 4))
    (is (string= topic (kf:create-topic consumer
                                        topic
                                        :partitions partitions
                                        :timeout-ms 5000
                                        :validate-only-p t)))
    (sleep 2)
    (is (= 0 (get-partitions topic)))))


(test delete-topic-with-consumer
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "delete-topic-with-consumer")
        (partitions 7))
    (is (string= topic (kf:create-topic consumer
                                        topic
                                        :partitions partitions
                                        :timeout-ms 5000)))
    (sleep 2)
    (is (= partitions (get-partitions topic)))

    (is (string= topic (kf:delete-topic consumer topic :timeout-ms 5000)))
    (sleep 2)
    (is (= 0 (get-partitions topic)))))

(test delete-topic-with-producer
  (let ((producer (make-instance
                   'kf:producer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "delete-topic-with-producer")
        (partitions 4))
    (is (string= topic (kf:create-topic producer
                                        topic
                                        :partitions partitions
                                        :timeout-ms 5000)))
    (sleep 2)
    (is (= partitions (get-partitions topic)))

    (is (string= topic (kf:delete-topic producer topic :timeout-ms 5000)))
    (sleep 2)
    (is (= 0 (get-partitions topic)))))


(test create-partitions-with-consumer
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "create-partitions-with-consumer")
        (old-partitions 7)
        (new-partitions 10))
    (is (string= topic (kf:create-topic consumer
                                        topic
                                        :partitions old-partitions)))
    (sleep 2)
    (is (= old-partitions (get-partitions topic)))

    (is (= new-partitions (kf:create-partitions consumer topic new-partitions)))
    (sleep 2)
    (is (= new-partitions (get-partitions topic)))))

(test create-partitions-with-producer
  (let ((producer (make-instance
                   'kf:producer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "create-partitions-with-producer")
        (old-partitions 11)
        (new-partitions 20))
    (is (string= topic (kf:create-topic producer
                                        topic
                                        :partitions old-partitions)))
    (sleep 2)
    (is (= old-partitions (get-partitions topic)))

    (is (= new-partitions (kf:create-partitions producer topic new-partitions)))
    (sleep 2)
    (is (= new-partitions (get-partitions topic)))))


(test describe-topic-with-consumer
  (let ((consumer (make-instance
                   'kf:consumer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "describe-topic-with-consumer"))
    (is (string= topic (kf:create-topic consumer topic)))
    (sleep 2)
    (let ((expected "CreateTime")
          (actual (cdr (assoc "message.timestamp.type"
                              (kf:describe-config consumer topic :topic)
                              :test #'string=))))
      (is (string= expected actual)))))

(test describe-topic-with-producer
  (let ((producer (make-instance
                   'kf:producer
                   :conf (kf:conf "bootstrap.servers" "kafka:9092")))
        (topic "describe-topic-with-producer"))
    (is (string= topic (kf:create-topic producer topic)))
    (sleep 2)
    (let ((expected "CreateTime")
          (actual (cdr (assoc "message.timestamp.type"
                              (kf:describe-config producer topic :topic)
                              :test #'string=))))
      (is (string= expected actual)))))

(test describe-broker-with-consumer
  (let* ((consumer (make-instance
                    'kf:consumer
                    :conf (kf:conf "bootstrap.servers" "kafka:9092")))
         (config (kf:describe-config consumer "1001" :broker)))
    (is (string= "kafka"
                 (cdr (assoc "advertised.host.name" config :test #'string=))))
    (is (string= "9092"
                 (cdr (assoc "advertised.port" config :test #'string=))))))

(test describe-broker-with-producer
  (let* ((producer (make-instance
                    'kf:producer
                    :conf (kf:conf "bootstrap.servers" "kafka:9092")))
         (config (kf:describe-config producer "1001" :broker)))
    (is (string= "kafka"
                 (cdr (assoc "advertised.host.name" config :test #'string=))))
    (is (string= "9092"
                 (cdr (assoc "advertised.port" config :test #'string=))))))


(test alter-topic-with-consumer
  (let* ((consumer (make-instance
                    'kf:consumer
                    :conf (kf:conf "bootstrap.servers" "kafka:9092")))
         (topic "alter-topic-with-consumer")
         (get-actual (lambda ()
                       (cdr (assoc "message.timestamp.type"
                                   (kf:describe-config consumer topic :topic)
                                   :test #'string=)))))
    (is (string= topic (kf:create-topic consumer topic)))
    (sleep 2)
    (is (string= "CreateTime" (funcall get-actual)))

    (kf:alter-config consumer
                     topic
                     '(("message.timestamp.type" . "LogAppendTime")))
    (sleep 2)
    (is (string= "LogAppendTime" (funcall get-actual)))))

(test alter-topic-with-producer
  (let* ((producer (make-instance
                    'kf:producer
                    :conf (kf:conf "bootstrap.servers" "kafka:9092")))
         (topic "alter-topic-with-producer")
         (get-actual (lambda ()
                       (cdr (assoc "message.timestamp.type"
                                   (kf:describe-config producer topic :topic)
                                   :test #'string=)))))
    (is (string= topic (kf:create-topic producer topic)))
    (sleep 2)
    (is (string= "CreateTime" (funcall get-actual)))

    (kf:alter-config producer
                     topic
                     '(("message.timestamp.type" . "LogAppendTime")))
    (sleep 2)
    (is (string= "LogAppendTime" (funcall get-actual)))))
