#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

lib=$bin/../lib/master
dist=$bin/../dist
resources=$bin/../resources


# HEAP_OPTS="-Xmx4096m -Xms2048m -XX:NewSize=1024m" # -d64 for 64-bit awesomeness
#HEAP_OPTS="-Xmx1g -Xms1g -XX:NewSize=256m"
# HEAP_OPTS="-Xmx1024m -Xms512m -XX:NewSize=128m"
 HEAP_OPTS="-Xmx512m -Xms256m -XX:NewSize=64m"
# GC_OPTS="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"
#GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JAVA_OPTS="-server -d64 -DclusterName=sensei"
MAIN_CLASS="com.sensei.search.cluster.client.SenseiClusterClient"



CLASSPATH=$resources/:$lib/*:$dist/*


java $JAVA_OPTS $HEAP_OPTS $GC_OPTS -classpath $CLASSPATH -Didx.dir=$idx -Dlog.home=$logs $MAIN_CLASS $1 $2 $3 $4
