# cl-rdkafka

A Common Lisp wrapper for [librdkafka](https://github.com/edenhill/librdkafka)
via [CFFI](https://common-lisp.net/project/cffi/manual/html_node/index.html).

The project structure is modelled after
[cl-charms](https://github.com/HiTECNOLOGYs/cl-charms).

The `cl-rdkafka/low-level` package wraps the
[librdkafka/rdkafka.h](https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.h)
header file.
