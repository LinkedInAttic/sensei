#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

cd $bin/..
protoc --java_out=sensei-core/src/main/java protobuf/*.proto