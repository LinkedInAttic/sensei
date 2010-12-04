#!/usr/bin/env bash

arg=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

severlib=$bin/../lib/server
serverconf=$bin/../server-conf
lib=$bin/../lib/master
dist=$bin/../dist
resources=$bin/../resources
logs=$bin/../logs

# HEAP_OPTS="-Xmx4096m -Xms2048m -XX:NewSize=1024m" # -d64 for 64-bit awesomeness
#HEAP_OPTS="-Xmx1g -Xms1g -XX:NewSize=256m"
# HEAP_OPTS="-Xmx1024m -Xms512m -XX:NewSize=128m"
 HEAP_OPTS="-Xmx512m -Xms256m -XX:NewSize=64m"
# GC_OPTS="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=y"
#GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JAVA_OPTS="-server -d64 -Dlog.home=$logs -Dconf.dir=$serverconf"

# Controls the backend service
PIDFILE=/tmp/sensei-server.pid

start_server()
{
 #kill `cat $$PIDFILE`
 if [ -f $PIDFILE ]; then
  echo "File $PIDFILE exists shutdown may not be proper"
  echo "Please check PID" `cat $PIDFILE`
  echo "Make sure the node is shutdown and the file" $PIDFILE "is removed before stating the node"
  return 1
 else
  echo "File $PIDFILE does not exists"
  mkdir $logs
  java $JAVA_OPTS $HEAP_OPTS $GC_OPTS -jar $severlib/zoie-server.jar >$logs/sensei-server.log > $logs/sensei-server.log &
  echo $! > ${PIDFILE}
  return 0
 fi
}

stop_server()
{
 echo killing `cat $PIDFILE` and wait for it to die. could take long
 kill `cat $PIDFILE`
 while ps -p `cat $PIDFILE`  > /dev/null; do sleep 1; done
 echo `cat $PIDFILE` killed
 echo remove ${PIDFILE}
 rm ${PIDFILE}
 echo done stop sensei server
 return 0
}


if [ $arg = "start" ]; then
  start_server
fi

if [ $arg = "stop" ]; then
  stop_server
fi

if [ $arg = "restart" ]; then
  stop_server
  start_server
fi

