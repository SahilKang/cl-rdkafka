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

(in-package #:test/high-level/message)

(defmacro with-producer (producer &body body)
  `(let ((,producer (cl-rdkafka/ll:rd-kafka-new
                     cl-rdkafka/ll:rd-kafka-producer
                     (cffi:null-pointer)
                     (cffi:null-pointer)
                     0)))
     (when (cffi:null-pointer-p ,producer)
       (error "~&Could not allocate new producer!"))
     (unwind-protect
          ,@body
       (unless (cffi:null-pointer-p ,producer)
         (cl-rdkafka/ll:rd-kafka-destroy ,producer)))))

(defmacro %with-topic (topic topic-name producer &body body)
  `(let ((,topic (cl-rdkafka/ll:rd-kafka-topic-new
                  ,producer
                  ,topic-name
                  (cffi:null-pointer))))
     (when (cffi:null-pointer-p ,topic)
       (error "~&Could not allocate new topic!"))
     (unwind-protect
          ,@body
       (unless (cffi:null-pointer-p ,topic)
         (cl-rdkafka/ll:rd-kafka-topic-destroy ,topic)))))

(defmacro with-topic (topic topic-name &body body)
  (let ((producer (gensym)))
    `(with-producer ,producer
       (%with-topic ,topic ,topic-name ,producer
         ,@body))))

(defmacro with-message (message
                        err
                        topic
                        partition
                        offset
                        payload
                        key
                        &body body)
  (let ((rd-topic (gensym))
        (c-key (gensym))
        (c-payload (gensym)))
    `(with-topic ,rd-topic ,topic
       (cffi:with-foreign-object
           (,message '(:struct cl-rdkafka/ll:rd-kafka-message))
         (cffi:with-foreign-strings ((,c-key ,key)
                                     (,c-payload ,payload))
           (cffi:with-foreign-slots
               ((cl-rdkafka/ll:err
                 cl-rdkafka/ll:rkt
                 cl-rdkafka/ll:partition
                 cl-rdkafka/ll:offset
                 cl-rdkafka/ll:key
                 cl-rdkafka/ll:key-len
                 cl-rdkafka/ll:payload
                 cl-rdkafka/ll:len)
                ,message
                (:struct cl-rdkafka/ll:rd-kafka-message))
             (setf
              cl-rdkafka/ll:err ,err
              cl-rdkafka/ll:rkt ,rd-topic
              cl-rdkafka/ll:partition ,partition
              cl-rdkafka/ll:offset ,offset
              cl-rdkafka/ll:key ,c-key
              cl-rdkafka/ll:key-len (length ,key)
              cl-rdkafka/ll:payload ,c-payload
              cl-rdkafka/ll:len (length ,payload)))
           ,@body)))))

(def-test message-constructor ()
  (let ((topic "test-topic")
        (partition 1)
        (offset 3)
        (key "key-1")
        (value "Hello World!")
        (string-serde (lambda (x)
                        (kf:bytes->object x 'string))))
    (with-message
        rd-message
        cl-rdkafka/ll:rd-kafka-resp-err-no-error
        topic
        partition
        offset
        value
        key
      (let ((message (make-instance 'kf:message
                                    :rd-kafka-message rd-message
                                    :key-serde string-serde
                                    :value-serde string-serde)))
        (is (and
             (string= topic (kf:topic message))
             (= partition (kf:partition message))
             (= offset (kf:offset message))
             (string= key (kf:key message))
             (string= value (kf:value message))))))))

(def-test bad-message-value ()
  (let ((topic "test-topic")
        (partition 1)
        (offset 3)
        (key "key-1")
        (value "Hello World!")
        (err cl-rdkafka/ll:rd-kafka-resp-err-not-enough-replicas)
        (string-serde (lambda (x)
                        (kf:bytes->object x 'string))))
    (with-message
        rd-message
        err
        topic
        partition
        offset
        value
        key
      (let ((message (make-instance 'kf:message
                                    :rd-kafka-message rd-message
                                    :key-serde string-serde
                                    :value-serde string-serde)))
        (signals kf:message-error
          (kf:value message))))))

(def-test bad-message-key ()
  (let ((topic "test-topic")
        (partition 1)
        (offset 3)
        (key "key-1")
        (value "Hello World!")
        (err cl-rdkafka/ll:rd-kafka-resp-err-not-enough-replicas)
        (string-serde (lambda (x)
                        (kf:bytes->object x 'string))))
    (with-message
        rd-message
        err
        topic
        partition
        offset
        value
        key
      (let ((message (make-instance 'kf:message
                                    :rd-kafka-message rd-message
                                    :key-serde string-serde
                                    :value-serde string-serde)))
        (signals kf:message-error
          (kf:key message))))))

(def-test bad-message-use-value ()
  (let ((topic "test-topic")
        (partition 1)
        (offset 3)
        (key "key-1")
        (value "Hello World!")
        (err cl-rdkafka/ll:rd-kafka-resp-err-not-enough-replicas)
        (string-serde (lambda (x)
                        (kf:bytes->object x 'string))))
    (with-message
        rd-message
        err
        topic
        partition
        offset
        value
        key
      (let* ((message (make-instance 'kf:message
                                     :rd-kafka-message rd-message
                                     :key-serde string-serde
                                     :value-serde string-serde))
             (actual-key (handler-bind
                             ((kf:message-error (lambda (c)
                                                  (use-value key))))
                           (kf:key message)))
             (actual-value (handler-bind
                               ((kf:message-error (lambda (c)
                                                    (use-value value))))
                             (kf:value message))))
        (is (and
             (string= topic (kf:topic message))
             (= partition (kf:partition message))
             (= offset (kf:offset message))
             (string= key actual-key)
             (string= value actual-value)))))))

(def-test message-error ()
  (let ((topic "test-topic")
        (partition 1)
        (offset 3)
        (key "key-1")
        (value "Hello World!")
        (err cl-rdkafka/ll:rd-kafka-resp-err-not-enough-replicas)
        (string-serde (lambda (x)
                        (kf:bytes->object x 'string))))
    (with-message
        rd-message
        err
        topic
        partition
        offset
        value
        key
      (let ((message (make-instance 'kf:message
                                    :rd-kafka-message rd-message
                                    :key-serde string-serde
                                    :value-serde string-serde))
            message-error)
        (handler-bind
            ((kf:message-error (lambda (c)
                                 (setf message-error c)
                                 (use-value nil))))
          (kf:value message))

        (is (and
             (string= topic (kf:topic message-error))
             (= partition (kf:partition message-error))
             (= offset (kf:offset message-error))
             (= (cl-rdkafka/ll:num err)
                (kf:error-code (kf:message-error message-error)))))))))
