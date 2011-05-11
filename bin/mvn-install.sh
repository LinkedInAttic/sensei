#!/bin/bash
#echo "installing jmxri 1.2.1"
#mvn install:install-file -Dfile=lib/jmxri-1.2.1.jar -DgroupId=com.sun.jmx -DartifactId=jmxri -Dversion=1.2.1 -Dpackaging=jar
#echo "installing jmxtools 1.2.1"
#mvn install:install-file -Dfile=lib/jmxtools-1.2.1.jar -DgroupId=com.sun.jdmk -DartifactId=jmxtools -Dversion=1.2.1 -Dpackaging=jar
#echo "installing bobo 2.5.1-SNAPSHOT"
#mvn install:install-file -Dfile=lib/bobo-browse-2.5.1-SNAPSHOT.jar -DgroupId=com.sna-projects.bobo -DartifactId=bobo-browse -Dversion=2.5.1-SNAPSHOT -Dpackaging=jar
#echo "installing zoie 2.5.1-SNAPSHOT"
#mvn install:install-file -Dfile=lib/zoie-2.5.1-SNAPSHOT.jar -DgroupId=com.sna-projects.zoie -DartifactId=zoie-core -Dversion=2.5.1-SNAPSHOT -Dpackaging=jar
#echo "installing kafka 0.0.6-SNAPSHOT"
#mvn install:install-file -Dfile=lib/kafka-0.0.6-SNAPSHOT.jar -DgroupId=com.sna-projects.kafka -DartifactId=kafka -Dversion=0.0.6-SNAPSHOT -Dpackaging=jar
echo "installing norbert 1.0.0-SNAPSHOT"
mvn install:install-file -Dfile=lib/norbert-2.8.1-0.6.8.jar -DgroupId=com.linkedin.norbert -DartifactId=norbert -Dversion=2.8.1-0.6.8 -Dpackaging=jar
