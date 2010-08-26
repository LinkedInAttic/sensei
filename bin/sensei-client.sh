#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

lib=$bin/../lib/master
dist=$bin/../dist
resources=$bin/../resources

CLASSPATH=$lib/fastutil.jar:$lib/log4j.jar:$lib/lucene-core.jar:\
$lib/protobuf-java.jar:$lib/bobo-browse-2.5.0.jar:$lib/kamikaze-2.0.0.jar:$lib/commons-logging.jar:\
$lib/netty-3.1.5.GA.jar:$lib/spring.jar:$lib/scala-library-2.8.0.jar:$lib/zoie-2.0.0-SNAPSHOT.jar:\
$lib/norbert-java-cluster_2.8.0-0.5-SNAPSHOT.jar:$lib/norbert-java-network_2.8.0-0.5-SNAPSHOT.jar:$lib/norbert-cluster_2.8.0-0.5-SNAPSHOT.jar:$lib/norbert-network_2.8.0-0.5-SNAPSHOT.jar:$lib/zookeeper-3.2.0.jar:$dist/sensei-0.0.1.jar:$resources


java -classpath $CLASSPATH -DclusterName=sensei com.sensei.search.cluster.client.SenseiClusterClient $1
