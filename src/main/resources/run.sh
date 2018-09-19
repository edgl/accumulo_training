#!/bin/bash
project_dir=$1
/usr/hdp/current/accumulo-client/bin/tool.sh ${project_dir}/target/accumulo-1.0-SNAPSHOT.jar $2 -p ${project_dir}/src/main/resources/ingestclient.properties  
