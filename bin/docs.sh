#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

pushd .
cd $bin/..

echo "building javadoc"
mvn javadoc:javadoc

mkdir $bin/../target

echo "creating output for javadoc"
mkdir $bin/../target/docs

tar -zcf $bin/../target/javadocs/sensei-core-javadoc.tar.gz $bin/../sensei-core/target/site
tar -zcf $bin/../target/javadocs/sensei-hadoop-indexing-javadoc.tar.gz $bin/../sensei-hadoop-indexing/target/site
tar -zcf $bin/../target/javadocs/sensei-java-client-javadoc.tar.gz $bin/../clients/java-client/target/site

#echo "build docbook"
#cd $bin/../docs
#mvn docbkx:generate-html

popd
