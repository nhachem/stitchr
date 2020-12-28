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
# nohup $STITCHR_ROOT/bash/runMoveDataSetList.sh web_sales_1,stores_sales_1,q21_1  > /tmp/logs/moveDataSetList.log &

## note this sets you to invoke the shell script from the stitchr root We need to fix...
## setup the environment
## source bash/stitchr_env.sh

source ./bash/stitchr_env.sh
# STITCHR_SCALA_VERSION=2.11
# STITCHR_SPARK_VERSION=2.4.6

echo $@
STITCHR_CLASS="com.stitchr.app.TransformDataSetList"

# for a cluster deployment better use deploy_mode
#    --deploy-mode client \
$SPARK_HOME/bin/spark-submit \
      --master $MASTER \
      --class $STITCHR_CLASS\
      --packages org.apache.spark:spark-avro_$STITCHR_SCALA_VERSION:$STITCHR_SPARK_VERSION \
      --conf spark.sql.hive.metastore.version=2.3.7 \
     "$STITCHR_JAR" \
     "$@"
