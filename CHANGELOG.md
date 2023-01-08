# Changelog

## [1.2.1] - 2023-01-08

### Fixed

 * When checking the return value's type, key and value producer
   serializers no longer traverse the return value if it's a
   `(vector (unsigned-byte 8))`.

 * Set `client.software.name` and `client.software.version` configs.

## [1.2.0] - 2022-12-30

### Added

 * Add support for offsets to `assign` and `assignment` consumer methods.
   [issue #67](https://github.com/SahilKang/cl-rdkafka/issues/67) from
   [@Uthar](https://github.com/Uthar).

## [1.1.0] - 2020-07-04

### Added

 * Add `seek`, `seek-to-beginning` and `seek-to-end` consumer methods.

 * Add support for transactions.

## [1.0.2] - 2020-04-20

### Fixed

 * Fix deadlock bug described in
   [issue #58](https://github.com/SahilKang/cl-rdkafka/issues/58) from
   [@kat-co](https://github.com/kat-co).

## [1.0.1] - 2020-01-14

### Fixed

 * Add `/usr/local/include` to `darwin` search path.
   [PR #55](https://github.com/SahilKang/cl-rdkafka/pull/55) from
   [@noname007](https://github.com/noname007).

## [1.0.0] - 2019-12-29

Initial release
