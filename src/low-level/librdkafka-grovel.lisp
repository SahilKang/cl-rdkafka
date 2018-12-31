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

(include "librdkafka/rdkafka.h")

(ctype size-t "size_t")

(ctype ssize-t "ssize_t")

(constant (rd-kafka-offset-beginning "RD_KAFKA_OFFSET_BEGINNING"))

(constant (rd-kafka-offset-end "RD_KAFKA_OFFSET_END"))

(constant (rd-kafka-offset-stored "RD_KAFKA_OFFSET_STORED"))

(constant (rd-kafka-offset-invalid "RD_KAFKA_OFFSET_INVALID"))

(constant (rd-kafka-offset-tail "RD_KAFKA_OFFSET_TAIL_BASE"))

(constant (rd-kafka-msg-f-free "RD_KAFKA_MSG_F_FREE"))

(constant (rd-kafka-msg-f-copy "RD_KAFKA_MSG_F_COPY"))

(constant (rd-kafka-msg-f-block "RD_KAFKA_MSG_F_BLOCK"))

(constant (rd-kafka-msg-f-partition "RD_KAFKA_MSG_F_PARTITION"))

(constant (rd-kafka-partition-ua "RD_KAFKA_PARTITION_UA"))

(constant (rd-kafka-purge-f-queue "RD_KAFKA_PURGE_F_QUEUE"))

(constant (rd-kafka-purge-f-inflight "RD_KAFKA_PURGE_F_INFLIGHT"))

(constant (rd-kafka-purge-f-non-blocking "RD_KAFKA_PURGE_F_NON_BLOCKING"))

(constant (rd-kafka-event-none "RD_KAFKA_EVENT_NONE"))

(constant (rd-kafka-event-dr "RD_KAFKA_EVENT_DR"))

(constant (rd-kafka-event-fetch "RD_KAFKA_EVENT_FETCH"))

(constant (rd-kafka-event-log "RD_KAFKA_EVENT_LOG"))

(constant (rd-kafka-event-error "RD_KAFKA_EVENT_ERROR"))

(constant (rd-kafka-event-rebalance "RD_KAFKA_EVENT_REBALANCE"))

(constant (rd-kafka-event-offset-commit "RD_KAFKA_EVENT_OFFSET_COMMIT"))

(constant (rd-kafka-event-stats "RD_KAFKA_EVENT_STATS"))

(constant (rd-kafka-event-createtopics-result
	   "RD_KAFKA_EVENT_CREATETOPICS_RESULT"))

(constant (rd-kafka-event-deletetopics-result
	   "RD_KAFKA_EVENT_DELETETOPICS_RESULT"))

(constant (rd-kafka-event-createpartitions-result
	   "RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT"))

(constant (rd-kafka-event-alterconfigs-result
	   "RD_KAFKA_EVENT_ALTERCONFIGS_RESULT"))

(constant (rd-kafka-event-describeconfigs-result
	   "RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT"))
