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

(defgeneric describe-config (client name type &key timeout-ms)
  (:documentation
   "Return an alist of config key-val pairs for NAME.

If TYPE is :BROKER, then NAME should be the broker-id, like \"1001\".
If TYPE is :TOPIC, then NAME should be the topic-name."))


(defun make-configresource (name type)
  (let ((resource-type (find-symbol
                        (format nil "RD-KAFKA-RESOURCE-~A" type)
                        'cl-rdkafka/ll)))
    (unless resource-type
      (error "~&Could not find enum for ~S" type))
    (cffi:with-foreign-string (buf name)
      (let ((configresource (cl-rdkafka/ll:rd-kafka-configresource-new
                             (symbol-value resource-type)
                             buf)))
        (when (cffi:null-pointer-p configresource)
          (error "~&Failed to allocate a new configresource pointer"))
        configresource))))

(defun %describe-config (rd-kafka-client name type timeout-ms)
  (let (admin-options configresource)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 configresource (make-configresource name type))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (mapcar
            (lambda (giant-alist)
              (let ((name (cdr (assoc 'name giant-alist)))
                    (value (cdr (assoc 'value giant-alist))))
                (unless name
                  (error "~&Name is nil"))
                (cons name value)))
            (first
             (perform-admin-op describeconfigs
                               rd-kafka-client
                               admin-options
                               configresource))))
      (when configresource
        (cl-rdkafka/ll:rd-kafka-configresource-destroy configresource))
      (when admin-options
        (cl-rdkafka/ll:rd-kafka-adminoptions-destroy admin-options)))))

(macrolet
    ((defdescribe (type)
       `(def-admin-methods
            describe-config
            (client (name string) (type (eql ,type)) &key (timeout-ms 5000))
          (%describe-config pointer name type timeout-ms))))
  (defdescribe :topic)
  (defdescribe :broker))
