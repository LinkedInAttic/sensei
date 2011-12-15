#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

home=`cd "$bin/..";pwd`

pushd .

cd $home

echo "building javadoc"
mvn javadoc:javadoc

mkdir $home/target

echo "creating output for javadoc"
mkdir $home/target/docs

pushd .
cd $home/sensei-core/target/site
tar -zcf $home/target/docs/sensei-core-javadoc.tar.gz apidocs
popd


pushd .
cd  $home/sensei-hadoop-indexing/target/site
tar -zcf $home/target/docs/sensei-hadoop-indexing-javadoc.tar.gz apidocs
popd

pushd .
cd $home/clients/java-client/target/site
tar -zcf $home/target/docs/sensei-java-client-javadoc.tar.gz apidocs
popd

#echo "build docbook"
#cd $bin/../docs
#mvn docbkx:generate-html

popd
