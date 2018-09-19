#!/bin/bash

HADOOP_CLASSPATH=`hadoop classpath`
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/hdp/2.6.5.0-292/accumulo/lib/*
hadoop jar target/accumulo-1.0-SNAPSHOT.jar $1
