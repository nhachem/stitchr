#!/bin/sh

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

## set the environments per your's
export USER_ROOT=/Users/nabilhachem
export USER_PERSIST_ROOT=$USER_ROOT/data
export STITCHR_ROOT=$USER_ROOT/repo/stitchr
export CONFIG_DIR=$USER_ROOT/demo
export DATA_DIR=$USER_PERSIST_ROOT/demo
export REGISTRY_DIR=$USER_ROOT/demo

export baseRegistryFolder=$REGISTRY_DIR/registry/
export baseConfigFolder=$CONFIG_DIR/config/
## using tpcds generated and adjusted data
export baseDataFolder=$DATA_DIR/tpcds/ ## for the demo


export defaultOutputDir=/tmp

## spark
# export SPARK_HOME="<spark home>"
## export SPARK_HOME=/Users/nabilhachem/sparkBin/spark-2.4.3-bin-hadoop2.7
export VERSION=0.1-SNAPSHOT
export MASTER=local[4]
export STITCHR_JAR=$STITCHR_ROOT/app/target/stitchr-app-$VERSION-jar-with-dependencies.jar
## set it up if JAVA_HOME is not set export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_211.jdk/Contents/Home
## export PATH=$SPARK_HOME/bin:$PATH
