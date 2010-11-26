#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

cd $bin/..
protoc --java_out=src protobuf/*.proto