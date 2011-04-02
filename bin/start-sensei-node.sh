#!/usr/bin/env bash

#usage="Usage: start-sensei-node.sh <id> <port> <partitions> <conf-dir>"

# if no args specified, show usage
#if [ $# -le 3 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

lib=$bin/../target/lib
dist=$bin/../target
resources=$bin/../resources
logs=$bin/../logs

# HEAP_OPTS="-Xmx4096m -Xms2048m -XX:NewSize=1024m" # -d64 for 64-bit awesomeness
HEAP_OPTS="-Xmx1g -Xms1g -XX:NewSize=256m"
# HEAP_OPTS="-Xmx1024m -Xms512m -XX:NewSize=128m"
# HEAP_OPTS="-Xmx512m -Xms256m -XX:NewSize=64m"
# GC_OPTS="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"
#GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JAVA_OPTS="-server -d64"
MAIN_CLASS="com.sensei.search.nodes.SenseiServer"


CLASSPATH=$resources/:$lib/*:$dist/*

PIDFILE=/tmp/sensei-search-node.pid

if [ -f $PIDFILE ]; then
  echo "File $PIDFILE exists shutdown may not be proper"
  echo "Please check PID" `cat $PIDFILE`
  echo "Make sure the node is shutdown and the file" $PIDFILE "is removed before stating the node"
 else
  echo "File $PIDFILE does not exists"
  java $JAVA_OPTS $HEAP_OPTS $GC_OPTS -classpath $CLASSPATH  -Dlog.home=$logs $MAIN_CLASS $1  > $logs/sensei-server.log > $logs/sensei-server.log &
  echo $! > ${PIDFILE}
 fi