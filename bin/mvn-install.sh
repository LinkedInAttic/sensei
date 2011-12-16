#!/bin/bash
echo "installing kafka 0.6.7"
mvn install:install-file -Dfile=../lib/kafka-0.7.6.jar -DgroupId=org.apache.kafka -DartifactId=kafka -Dversion=0.7.6 -Dpackaging=jar
