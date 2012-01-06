#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

home=`cd "$bin/..";pwd`


rm -rf $1/javadoc/*
mkdir $1/javadoc/sensei-core
pushd .
cd $1/javadoc/sensei-core
tar -zxf $home/target/docs/sensei-core-javadoc.tar.gz
popd

mkdir $1/javadoc/sensei-java-client
pushd .
cd $1/javadoc/sensei-java-client
tar -zxf $home/target/docs/sensei-java-client-javadoc.tar.gz 
popd

mkdir $1/javadoc/sensei-hadoop-indexing
pushd .
cd $1/javadoc/sensei-hadoop-indexing
tar -zxf $home/target/docs/sensei-hadoop-indexing-javadoc.tar.gz 
popd 

