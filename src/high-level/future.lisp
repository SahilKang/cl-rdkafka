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

;; this future class exists to prevent the gc from collecting the
;; producer/consumer before the returned promise is fulfilled:
;;
;; (let (lparallel-promise)
;;   (let ((producer (make-instance 'kf:producer ...)))
;;     (setf lparallel-promise (kf:send producer ...)))
;;   ...
;;   (do-stuff lparallel-promise)
;;   ...)
;;
;; the lparallel-promise needs the producer object to exist while it's
;; being fulfilled, hence the reason for the client slot below

(defclass future ()
  ((promise
    :initform (error "Must supply lparallel promise")
    :initarg :promise
    :documentation "lparallel promise that backs this future object.")
   (client
    :initform (error "Must supply producer or consumer")
    :initarg :client
    :type (or consumer producer)
    :documentation
    "Reference to a producer/consumer to prevent gc during promise fulfillment."))
  (:documentation
   "A future to hold the result of an async operation.

Example:

(let ((future (kf:send producer \"topic\" \"message\")))
  (kf:donep future) ;; => nil
  (kf:value future) ;; => #<MESSAGE {1005BE9D23}>
  (kf:donep future) ;; => t

  (let ((new-future (kf:then future
                             (lambda (message err)
                               (when err
                                 (error err))
                               (kf:value message)))))
    (kf:value new-future))) ;; => \"message\""))

(defgeneric donep (future))

(defgeneric then (future callback))

(defmethod value ((future future))
  "Wait until FUTURE is done and return its value or signal its condition."
  (with-slots (promise) future
    (let ((value (lparallel:force promise)))
      (if (typep value 'condition)
          (error value)
          value))))

(defmethod donep ((future future))
  "Determine if FUTURE is done processing."
  (with-slots (promise) future
    (lparallel:fulfilledp promise)))

(defmethod then ((future future) (callback function))
  "Return a new FUTURE that calls CALLBACK when current future completes.

CALLBACK should be a binary function accepting the positional args:
  1) value: the value that the current future evaluates to, or nil
            when it signals a condition.
  2) condition: the condition signalled by the current future, or nil
                when it does not signal a condition.

CALLBACK is called in a background thread."
  (with-slots (promise client) future
    (make-instance 'future
                   :client client
                   :promise (let ((lparallel:*kernel* +kernel+))
                              (lparallel:future
                                (let ((value (lparallel:force promise)))
                                  (if (typep value 'condition)
                                      (funcall callback nil value)
                                      (funcall callback value nil))))))))
