#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

home=`cd "$bin/..";pwd`

pushd .
cd $home

mvn -Dmaven.test.skip=true package

cd example/tweets
rm -rf target
rm -rf conf/ext
mvn package
popd
