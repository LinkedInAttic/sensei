#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "installing kafka 0.7.6"

mvn install:install-file -Dfile=$bin/../lib/kafka-0.7.6.jar -DgroupId=org.apache.kafka -DartifactId=kafka -Dversion=0.7.6 -Dpackaging=jar
