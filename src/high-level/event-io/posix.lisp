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

(cffi:defcstruct pollfd
  (fd :int)
  (events :short)
  (revents :short))

(cffi:defcfun ("poll" posix-poll) :int
  (fds :pointer)
  (nfds nfds-t)
  (timeout :int))

(cffi:defcfun ("pipe" posix-pipe) :int
  (fds :pointer))

(cffi:defcfun ("close" posix-close) :int
  (fd :int))

(cffi:defcfun ("read" posix-read) cl-rdkafka/ll:ssize-t
  (fd :int)
  (buf :pointer)
  (count cl-rdkafka/ll:size-t))

(cffi:defcfun ("write" posix-write) cl-rdkafka/ll:ssize-t
  (fd :int)
  (buf :pointer)
  (count cl-rdkafka/ll:size-t))
