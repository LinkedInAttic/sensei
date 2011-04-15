#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`


if [ $# -le 0 ]; then
  echo "Usage: conf-dir"
  exit 1
fi


if [[ ! -d $bin/../$1.old ]]; then
  echo "$bin/../$1.old does not exist, cannot roll back"
  exit 1
fi


echo "stopping sensei server"

$bin/kill.sh

echo "deleting current build"
rm -rf $bin/../$1

echo "rolling back"
mv $bin/../$1.old $bin/../$1

cd $bin/../

echo "starting sensei server"
$bin/start-sensei-node.sh $1