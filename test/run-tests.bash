#! /usr/bin/env bash

# wait for kafka service to come up
sleep 5

# produce messages for consumer test
echo -n 'Hello|World|!' \
    | kafkacat -P -D '|' -b 'kafka:9092' -t 'consumer-test-topic'

# run tests
sbcl --noinform --core /app/test-image
