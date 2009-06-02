#!/bin/bash

vp_dir=$(dirname $0)

for file in $vp_dir/lib/*.jar;
do
	CLASSPATH=$CLASSPATH:$file
done

CLASSPATH="$CLASSPATH:src/main/groovy"

export CLASSPATH

export JAVA_OPTS=-Xmx4g

groovy -cp $CLASSPATH $@
