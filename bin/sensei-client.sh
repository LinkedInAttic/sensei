#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

lib=$bin/../lib/master
dist=$bin/../dist
resources=$bin/../resources

logs=${bin}/../logs

CLASSPATH=$lib/*:$dist/*:$resources


java -classpath $CLASSPATH -DclusterName=sensei -Dlog.home=$logs com.sensei.search.cluster.client.SenseiClusterClient $1
