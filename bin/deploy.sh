#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

if [ $# -le 1 ]; then
  echo "Usage: tarball conf-dir"
  exit 1
fi

if [[ ! -f $1 ]]; then
  echo "$1 does not exist, nothing to do..."
  exit 0
fi

echo "stopping sensei server"

$bin/kill.sh

if [[ -d $bin/../$2.old ]]; then
	echo "removing old backup"
	rm -rf $bin/../$2.old
fi

if [[ -d $bin/../$2 ]]; then
	echo "backing up"
	mv $bin/../$2 $bin/../$2.old
fi

cd $bin/../
tar -zxf $1

$bin/start-sensei-node.sh $2
