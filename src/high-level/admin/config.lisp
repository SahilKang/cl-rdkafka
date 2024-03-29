;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
;;; Copyright 2023 Google LLC
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

;; TODO change describe-config and alter-config to:
;;   * describe-topic
;;   * describe-broker
;;   * alter-topic

(defgeneric describe-config (client name type &key timeout-ms)
  (:documentation
   "Return an alist of config key-val pairs for NAME.

If TYPE is :BROKER, then NAME should be the broker-id, like \"1001\".
If TYPE is :TOPIC, then NAME should be the topic-name."))

(defgeneric alter-config (client topic config &key timeout-ms)
  (:documentation
   "Alter config of TOPIC according to CONFIG, reverting to defaults for unspecified configs."))

(defgeneric get-conf (client)
  (:documentation
   "Return an alist describing the client config of CLIENT."))


(defun make-configresource (name type)
  (let ((resource-type (find-symbol
                        (format nil "RD-KAFKA-RESOURCE-~A" type)
                        'cl-rdkafka/ll)))
    (unless resource-type
      ;; TODO do this at compile-time
      (error "~&Could not find enum for ~S" type))
    (cffi:with-foreign-string (buf name)
      (let ((configresource (cl-rdkafka/ll:rd-kafka-configresource-new
                             (symbol-value resource-type)
                             buf)))
        (when (cffi:null-pointer-p configresource)
          (error 'allocation-error :name "configresource"))
        configresource))))

(defun giant-alist->decent-alist (giant-alist)
  (mapcar
   (lambda (giant)
     (let ((name (cdr (assoc 'name giant)))
           (value (cdr (assoc 'value giant))))
       (unless name
         (error "~&Name is nil"))
       (cons name value)))
   (first giant-alist)))

(defun %describe-config (rd-kafka-client name type timeout-ms)
  (let (admin-options configresource)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 configresource (make-configresource name type))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (giant-alist->decent-alist
            (perform-admin-op describeconfigs
                              rd-kafka-client
                              admin-options
                              configresource)))
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


(defun set-keyvals (configresource alist)
  (map nil
       (lambda (cons)
         (destructuring-bind (name . value)
             cons
           (cffi:with-foreign-strings ((name name) (value value))
             (let ((err (cl-rdkafka/ll:rd-kafka-configresource-set-config
                         configresource
                         name
                         value)))
               (unless (eq err 'cl-rdkafka/ll:rd-kafka-resp-err-no-error)
                 (error 'kafka-error
                        :description
                        (format nil "Failed to set config: `~A`"
                                (cl-rdkafka/ll:rd-kafka-err2str err))))))))
       alist))

(defun %alter-config (rd-kafka-client name config timeout-ms)
  (let (admin-options configresource)
    (unwind-protect
         (cffi:with-foreign-object (errstr :char +errstr-len+)
           (setf admin-options (make-admin-options rd-kafka-client)
                 configresource (make-configresource name 'topic))
           (set-timeout admin-options timeout-ms errstr +errstr-len+)
           (set-keyvals configresource config)
           (giant-alist->decent-alist
            (perform-admin-op alterconfigs
                              rd-kafka-client
                              admin-options
                              configresource)))
      (when configresource
        (cl-rdkafka/ll:rd-kafka-configresource-destroy configresource))
      (when admin-options
        (cl-rdkafka/ll:rd-kafka-adminoptions-destroy admin-options)))))

(def-admin-methods
    alter-config
    (client (topic string) (config list) &key (timeout-ms 5000))
  (%alter-config pointer topic config timeout-ms))


(def-admin-methods get-conf (client)
  (let ((conf (cl-rdkafka/ll:rd-kafka-conf pointer))
        kv-pairs
        *count)
    (unwind-protect
         (cffi:with-foreign-object (count :pointer)
           (setf kv-pairs (cl-rdkafka/ll:rd-kafka-conf-dump conf count)
                 *count (cffi:mem-ref count 'cl-rdkafka/ll:size-t))
           (loop
              with key

              for i below *count
              for string = (cffi:mem-aref kv-pairs :string i)

              if (evenp i) do (setf key string)
              else collect (cons key string)))
      (when kv-pairs
        (cl-rdkafka/ll:rd-kafka-conf-dump-free kv-pairs *count)))))
