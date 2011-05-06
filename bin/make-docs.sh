#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

if [ $# -le 0 ]; then
  echo "default target: html"
  target=generate-html
elif [[ $1 == "html" ]]; then
  target=generate-html
elif [[ $1 == "pdf" ]]; then
  target=generate-pdf	
else
  echo "unsupported doc output type: $1"
  echo "usage: html or pdf"
  exit 1 
fi

pushd .

cd $bin/../docs

echo "building documentation: $target"

mvn docbkx:$target

echo "done building documentation"

popd
