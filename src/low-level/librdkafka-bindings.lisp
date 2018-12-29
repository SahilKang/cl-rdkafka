;;; ===========================================================================
;;; Copyright (C) 2018 Sahil Kang <sahil.kang@asilaycomputing.com>
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
;;; ===========================================================================

(in-package #:cl-rdkafka/low-level)

(define-foreign-library librdkafka
  (t (:default "librdkafka")))

(use-foreign-library librdkafka)

(defcfun "rd_kafka_version" :int)

(defcfun "rd_kafka_version_str" :string)

(defcenum rd-kafka-type
  :rd-kafka-producer
  :rd-kafka-consumer)

(defcenum rd-kafka-timestamp-type
  :rd-kafka-timestamp-not-available
  :rd-kafka-timestamp-create-time
  :rd-kafka-timestamp-log-append-time)

(defcfun "rd_kafka_get_debug_contexts" :string)

(defcenum rd-kafka-resp-err
  (:rd-kafka-resp-err--begin -200)
  (:rd-kafka-resp-err--bad-msg -199)
  (:rd-kafka-resp-err--bad-compression -198)
  (:rd-kafka-resp-err--destroy -197)
  (:rd-kafka-resp-err--fail -196)
  (:rd-kafka-resp-err--transport -195)
  (:rd-kafka-resp-err--crit-sys-resource -194)
  (:rd-kafka-resp-err--resolve -193)
  (:rd-kafka-resp-err--msg-timed-out -192)
  (:rd-kafka-resp-err--partition-eof -191)
  (:rd-kafka-resp-err--unknown-partition -190)
  (:rd-kafka-resp-err--fs -189)
  (:rd-kafka-resp-err--unknown-topic -188)
  (:rd-kafka-resp-err--all-brokers-down -187)
  (:rd-kafka-resp-err--invalid-arg -186)
  (:rd-kafka-resp-err--timed-out -185)
  (:rd-kafka-resp-err--queue-full -184)
  (:rd-kafka-resp-err--isr-insuff -183)
  (:rd-kafka-resp-err--node-update -182)
  (:rd-kafka-resp-err--ssl -181)
  (:rd-kafka-resp-err--wait-coord -180)
  (:rd-kafka-resp-err--unknown-group -179)
  (:rd-kafka-resp-err--in-progress -178)
  (:rd-kafka-resp-err--prev-in-progress -177)
  (:rd-kafka-resp-err--existing-subscription -176)
  (:rd-kafka-resp-err--assign-partitions -175)
  (:rd-kafka-resp-err--revoke-partitions -174)
  (:rd-kafka-resp-err--conflict -173)
  (:rd-kafka-resp-err--state -172)
  (:rd-kafka-resp-err--unknown-protocol -171)
  (:rd-kafka-resp-err--not-implemented -170)
  (:rd-kafka-resp-err--authentication -169)
  (:rd-kafka-resp-err--no-offset -168)
  (:rd-kafka-resp-err--outdated -167)
  (:rd-kafka-resp-err--timed-out-queue -166)
  (:rd-kafka-resp-err--unsupported-feature -165)
  (:rd-kafka-resp-err--wait-cache -164)
  (:rd-kafka-resp-err--intr -163)
  (:rd-kafka-resp-err--key-serialization -162)
  (:rd-kafka-resp-err--value-serialization -161)
  (:rd-kafka-resp-err--key-deserialization -160)
  (:rd-kafka-resp-err--value-deserialization -159)
  (:rd-kafka-resp-err--partial -158)
  (:rd-kafka-resp-err--read-only -157)
  (:rd-kafka-resp-err--noent -156)
  (:rd-kafka-resp-err--underflow -155)
  (:rd-kafka-resp-err--invalid-type -154)
  (:rd-kafka-resp-err--retry -153)
  (:rd-kafka-resp-err--purge-queue -152)
  (:rd-kafka-resp-err--purge-inflight -151)
  (:rd-kafka-resp-err--fatal -150)
  (:rd-kafka-resp-err--inconsistent -149)
  (:rd-kafka-resp-err--gapless-guarantee -148)
  (:rd-kafka-resp-err--max-poll-exceeded -147)
  (:rd-kafka-resp-err--end -100)
  (:rd-kafka-resp-err-unknown -1)
  (:rd-kafka-resp-err-no-error 0)
  (:rd-kafka-resp-err-offset-out-of-range 1)
  (:rd-kafka-resp-err-invalid-msg 2)
  (:rd-kafka-resp-err-unknown-topic-or-part 3)
  (:rd-kafka-resp-err-invalid-msg-size 4)
  (:rd-kafka-resp-err-leader-not-available 5)
  (:rd-kafka-resp-err-not-leader-for-partition 6)
  (:rd-kafka-resp-err-request-timed-out 7)
  (:rd-kafka-resp-err-broker-not-available 8)
  (:rd-kafka-resp-err-replica-not-available 9)
  (:rd-kafka-resp-err-msg-size-too-large 10)
  (:rd-kafka-resp-err-stale-ctrl-epoch 11)
  (:rd-kafka-resp-err-offset-metadata-too-large 12)
  (:rd-kafka-resp-err-network-exception 13)
  (:rd-kafka-resp-err-group-load-in-progress 14)
  (:rd-kafka-resp-err-group-coordinator-not-available 15)
  (:rd-kafka-resp-err-not-coordinator-for-group 16)
  (:rd-kafka-resp-err-topic-exception 17)
  (:rd-kafka-resp-err-record-list-too-large 18)
  (:rd-kafka-resp-err-not-enough-replicas 19)
  (:rd-kafka-resp-err-not-enough-replicas-after-append 20)
  (:rd-kafka-resp-err-invalid-required-acks 21)
  (:rd-kafka-resp-err-illegal-generation 22)
  (:rd-kafka-resp-err-inconsistent-group-protocol 23)
  (:rd-kafka-resp-err-invalid-group-id 24)
  (:rd-kafka-resp-err-unknown-member-id 25)
  (:rd-kafka-resp-err-invalid-session-timeout 26)
  (:rd-kafka-resp-err-rebalance-in-progress 27)
  (:rd-kafka-resp-err-invalid-commit-offset-size 28)
  (:rd-kafka-resp-err-topic-authorization-failed 29)
  (:rd-kafka-resp-err-group-authorization-failed 30)
  (:rd-kafka-resp-err-cluster-authorization-failed 31)
  (:rd-kafka-resp-err-invalid-timestamp 32)
  (:rd-kafka-resp-err-unsupported-sasl-mechanism 33)
  (:rd-kafka-resp-err-illegal-sasl-state 34)
  (:rd-kafka-resp-err-unsupported-version 35)
  (:rd-kafka-resp-err-topic-already-exists 36)
  (:rd-kafka-resp-err-invalid-partitions 37)
  (:rd-kafka-resp-err-invalid-replication-factor 38)
  (:rd-kafka-resp-err-invalid-replica-assignment 39)
  (:rd-kafka-resp-err-invalid-config 40)
  (:rd-kafka-resp-err-not-controller 41)
  (:rd-kafka-resp-err-invalid-request 42)
  (:rd-kafka-resp-err-unsupported-for-message-format 43)
  (:rd-kafka-resp-err-policy-violation 44)
  (:rd-kafka-resp-err-out-of-order-sequence-number 45)
  (:rd-kafka-resp-err-duplicate-sequence-number 46)
  (:rd-kafka-resp-err-invalid-producer-epoch 47)
  (:rd-kafka-resp-err-invalid-txn-state 48)
  (:rd-kafka-resp-err-invalid-producer-id-mapping 49)
  (:rd-kafka-resp-err-invalid-transaction-timeout 50)
  (:rd-kafka-resp-err-concurrent-transactions 51)
  (:rd-kafka-resp-err-transaction-coordinator-fenced 52)
  (:rd-kafka-resp-err-transactional-id-authorization-failed 53)
  (:rd-kafka-resp-err-security-disabled 54)
  (:rd-kafka-resp-err-operation-not-attempted 55)
  (:rd-kafka-resp-err-kafka-storage-error 56)
  (:rd-kafka-resp-err-log-dir-not-found 57)
  (:rd-kafka-resp-err-sasl-authentication-failed 58)
  (:rd-kafka-resp-err-unknown-producer-id 59)
  (:rd-kafka-resp-err-reassignment-in-progress 60)
  (:rd-kafka-resp-err-delegation-token-auth-disabled 61)
  (:rd-kafka-resp-err-delegation-token-not-found 62)
  (:rd-kafka-resp-err-delegation-token-owner-mismatch 63)
  (:rd-kafka-resp-err-delegation-token-request-not-allowed 64)
  (:rd-kafka-resp-err-delegation-token-authorization-failed 65)
  (:rd-kafka-resp-err-delegation-token-expired 66)
  (:rd-kafka-resp-err-invalid-principal-type 67)
  (:rd-kafka-resp-err-non-empty-group 68)
  (:rd-kafka-resp-err-group-id-not-found 69)
  (:rd-kafka-resp-err-fetch-session-id-not-found 70)
  (:rd-kafka-resp-err-invalid-fetch-session-epoch 71)
  (:rd-kafka-resp-err-listener-not-found 72)
  (:rd-kafka-resp-err-topic-deletion-disabled 73)
  (:rd-kafka-resp-err-unsupported-compression-type 74)
  :rd-kafka-resp-err-end-all)

(defcstruct rd-kafka-err-desc
  (code rd-kafka-resp-err)
  (name :string)
  (char :string))

(defcfun "rd_kafka_get_err_descs" :void
  (rd-kafka-err-desc :pointer)
  (cntp :pointer))

(defcfun "rd_kafka_err2str" :string
  (err rd-kafka-resp-err))

(defcfun "rd_kafka_err2name" :string
  (err rd-kafka-resp-err))

(defcfun "rd_kafka_last_error" rd-kafka-resp-err)

(defcfun "rd_kafka_errno2err" rd-kafka-resp-err
  (errnox :int))

(defcfun "rd_kafka_errno" :int)

(defcfun "rd_kafka_fatal_error" rd-kafka-resp-err
  (rdk :pointer)
  (errstr :string)
  (errstr-size size-t))

(defcstruct rd-kafka-topic-partition
  (topic :string)
  (partition :int32)
  (offset :int64)
  (metadata :pointer)
  (metadata-size size-t)
  (opaque :pointer)
  (err rd-kafka-resp-err)
  (private :pointer))

(defcfun "rd_kafka_topic_partition_destroy" :void
  (rktpar :pointer))

(defcstruct rd-kafka-topic-partition-list
  (cnt :int)
  (size :int)
  (elems :pointer))

(defcfun "rd_kafka_topic_partition_list_new" :pointer
  (size :int))

(defcfun "rd_kafka_topic_partition_list_destroy" :void
  (rkparlist :pointer))

(defcfun "rd_kafka_topic_partition_list_add" :pointer
  (rktparlist :pointer)
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_topic_partition_list_add_range" :void
  (rktparlist :pointer)
  (topic :string)
  (start :int32)
  (stop :int32))

(defcfun "rd_kafka_topic_partition_list_del" :int
  (rktparlist :pointer)
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_topic_partition_list_del_by_idx" :int
  (rktparlist :pointer)
  (idx :int))

(defcfun "rd_kafka_topic_partition_list_copy" :pointer
  (src :pointer))

(defcfun "rd_kafka_topic_partition_list_set_offset" rd-kafka-resp-err
  (rktparlist :pointer)
  (topic :string)
  (partition :int32)
  (offset :int64))

(defcfun "rd_kafka_topic_partition_list_find" :pointer
  (rktparlist :pointer)
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_topic_partition_list_sort" :void
  (rktparlist :pointer)
  (cmp :pointer)
  (opaque :pointer))

(defcenum rd-kafka-vtype
  :rd-kafka-vtype-end
  :rd-kafka-vtype-topic
  :rd-kafka-vtype-rkt
  :rd-kafka-vtype-partition
  :rd-kafka-vtype-value
  :rd-kafka-vtype-key
  :rd-kafka-vtype-opaque
  :rd-kafka-vtype-msgflags
  :rd-kafka-vtype-timestamp
  :rd-kafka-vtype-header
  :rd-kafka-vtype-headers)

(defcfun "rd_kafka_headers_new" :pointer
  (initial-count size-t))

(defcfun "rd_kafka_headers_destroy" :void
  (hdrs :pointer))

(defcfun "rd_kafka_headers_copy" :pointer
  (src :pointer))

(defcfun "rd_kafka_header_add" rd-kafka-resp-err
  (hdrs :pointer)
  (name :string)
  (name-size ssize-t)
  (value :pointer)
  (value-size ssize-t))

(defcfun "rd_kafka_header_remove" rd-kafka-resp-err
  (hdrs :pointer)
  (name :string))

(defcfun "rd_kafka_header_get_last" rd-kafka-resp-err
  (hdrs :pointer)
  (name :string)
  (valuep :pointer)
  (sizep :pointer))

(defcfun "rd_kafka_header_get" rd-kafka-resp-err
  (hdrs :pointer)
  (idx size-t)
  (name :string)
  (valuep :pointer)
  (sizep :pointer))

(defcfun "rd_kafka_header_get_all" rd-kafka-resp-err
  (hdrs :pointer)
  (idx size-t)
  (namep :pointer)
  (valuep :pointer)
  (sizep :pointer))

(defcstruct rd-kafka-message
  (err rd-kafka-resp-err)
  (rkt :pointer)
  (partition :int32)
  (payload :pointer)
  (len size-t)
  (key :pointer)
  (key-ken size-t)
  (offset :int64)
  (private :pointer))
