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

(asdf:defsystem #:cl-rdkafka
  :description
  "A client library for Apache Kafka based on librdkafka CFFI bindings."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:cffi #:trivial-garbage #:bordeaux-threads #:lparallel)
  :defsystem-depends-on (#:cffi-grovel)
  :in-order-to ((test-op (test-op #:cl-rdkafka/test)))
  :build-pathname "cl-rdkafka"
  :pathname "src"
  :components
  ((:module "low-level"
            :serial t
            :components
            ((:file "package")
             (:cffi-grovel-file "librdkafka-grovel")
             (:file "librdkafka-bindings")))
   (:module "high-level"
            :depends-on ("low-level")
            :components
            ((:file "package")
             (:file "common" :depends-on ("package"))
             (:file "conditions" :depends-on ("package"))
             (:file "conf" :depends-on ("common"))
             (:file "serde" :depends-on ("common"))
             (:file "message" :depends-on ("common" "conditions"))
             (:module "event-io"
                      :depends-on ("package" "conditions")
                      :components
                      ((:cffi-grovel-file "posix-grovel")
                       (:file "posix" :depends-on ("posix-grovel"))
                       (:file "kernel" :depends-on ("posix"))))
             (:file "future" :depends-on ("event-io"))
             (:file "toppar" :depends-on ("common" "conditions"))
             (:file "consumer" :depends-on ("conf"
                                            "conditions"
                                            "serde"
                                            "message"
                                            "event-io"
                                            "future"
                                            "toppar"))
             (:file "producer" :depends-on ("consumer"))
             (:module "admin"
                      :depends-on ("consumer" "producer")
                      :components
                      ((:file "common")
                       (:file "create-topic" :depends-on ("common"))
                       (:file "delete-topic" :depends-on ("common"))
                       (:file "create-partitions" :depends-on ("common"))
                       (:file "config" :depends-on ("common"))
                       (:file "cluster-metadata" :depends-on ("common"))
                       (:file "group-info" :depends-on ("common"))))))))


(asdf:defsystem #:cl-rdkafka/test
  :description "Tests for cl-rdkafka."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:cl-rdkafka #:babel #:1am)
  :perform (test-op (op sys) (uiop:symbol-call :1am :run))
  :pathname "test"
  :components
  ((:file "common")
   (:module "low-level"
            :depends-on ("common")
            :components
            ((:file "unit-test")
             (:file "producer")
             (:file "consumer")))
   (:module "high-level"
            :depends-on ("common")
            :components
            ((:file "consumer")
             (:file "producer")
             (:file "produce->consume")
             (:file "admin")
             (:file "headers")
             (:file "transactions")))))
