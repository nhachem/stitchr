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

// need tlo fix this through config
export defaultOutputDir="<persistence temp space>"
export USER_ROOT="<user home>"
export USER_PERSIST_ROOT="<usually hdfs>"
export STITCHR_ROOT="<path to code base>"
export CONFIG_DIR="<pathe to where the config directory is>"
export DATA_DIR="<path-to-root--data directory>" ## usually $USER_PERSIST_ROOT/data/...
export REGISTRY_DIR="<path to where the registry directory is>"

export baseRegistryFolder=$REGISTRY_DIR/registry/
export baseConfigFolder=$CONFIG_DIR/config/
## using tpcds generated and adjusted data
export baseDataFolder=$DATA_DIR/tpcds/ ## for the demo

## if you are using a arget database set the proper values for the user and password.
## we will deprecate and manage differently when we expand to multiple target db engines
export jdbcUsername="<db user>"
export jdbcPassword="<db password>"
