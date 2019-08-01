#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

## example run
# nohup $STITCHR_ROOT/bash/runLoad2Lake.sh web_sales,postgresql_1_store_sales,postgresql_1_q21  > /tmp/logs/load2Lake.log &

## note this sets you to invoke the shell script from the stitchr root We need to fix


## you need to change those parameters
USER_ROOT=/Users/nabilhachem
STITCHR_ROOT=$USER_ROOT/repo/stitchr

source $STITCHR_ROOT/bash/env_demo.sh

## set it up if JAVA_HOME is not set export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_211.jdk/Contents/Home

SPARK_HOME=/Users/nabilhachem/sparkBin/spark-2.4.3-bin-hadoop2.7
STITCHR_JAR=$STITCHR_ROOT/app/target/stitchr-app-0.1-SNAPSHOT-jar-with-dependencies.jar
MASTER=local[4]

    
echo $@
STITCHR_CLASS="com.stitchr.app.ThreadedIngestService"

# for a cluster deployment better use deploy_mode
#    --deploy-mode client \
## --num-executors 3 \
## --conf core-per-executor=4 \
$SPARK_HOME/bin/spark-submit \
     --master $MASTER \
     --packages org.apache.spark:spark-avro_2.11:2.4.3 \
     --class $STITCHR_CLASS\
     "$STITCHR_JAR" \
     "$@"

