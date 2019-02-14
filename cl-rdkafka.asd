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

(asdf:defsystem #:cl-rdkafka
  :description "CFFI bindings for librdkafka."
  :version "0.0.1"
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :licence "GPLv3"
  :depends-on (#:cffi #:babel #:trivial-garbage)
  :defsystem-depends-on (#:cffi-grovel)
  :in-order-to ((test-op (test-op :cl-rdkafka/test)))
  :build-pathname "cl-rdkafka"
  :components
  ((:module
    "src"
    :components
    ((:module
      "low-level"
      :serial t
      :components
      ((:file "package")
       (:cffi-grovel-file "librdkafka-grovel")
       (:file "librdkafka-bindings")))
     (:module
      "high-level"
      :depends-on ("low-level")
      :components
      ((:file "package")
       (:file "serde" :depends-on ("package"))
       (:file "kafka-error" :depends-on ("package"))
       (:file "message" :depends-on ("kafka-error"))))))))

(asdf:defsystem :cl-rdkafka/test
  :description "Tests for cl-rdkafka."
  :version "0.0.1"
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :licence "GPLv3"
  :depends-on (#:cl-rdkafka #:fiveam)
  :perform (test-op (op sys) (uiop:symbol-call :fiveam :run-all-tests))
  :components
  ((:module
    "test"
    :serial t
    :components
    ((:file "package")
     (:module
      "low-level"
      :components
      ((:file "unit-test")
       (:file "producer")
       (:file "consumer")))
     (:module
      "high-level"
      :components
      ((:file "serde")
       (:file "kafka-error")))))))

#+sb-core-compression
(defmethod asdf:perform ((op asdf:image-op) (sys asdf:system))
  (uiop:dump-image (asdf:output-file op sys) :executable t :compression 9))
