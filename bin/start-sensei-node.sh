#!/usr/bin/env bash

#usage="Usage: start-sensei-node.sh <id> <port> <partitions> <conf-dir>"

# if no args specified, show usage
#if [ $# -le 3 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

lib=$bin/../lib/master
dist=$bin/../dist
resources=$bin/../resources
logs=${bin}/../logs

idx=$bin/../data/cardata

CLASSPATH=$resources:$lib/*:$dist/*


java -classpath $CLASSPATH -Didx.dir=$idx -Dlog.home=$logs com.sensei.search.nodes.SenseiServer $1 $2 $3 $4
