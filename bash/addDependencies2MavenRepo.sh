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


## run from base of code
## add jars to the maven repo assumes you have the jars available under jars...

## this is currently needed to compile the scala docs....

mvn install:install-file \
    -DgroupId=com.teradata.jdbc \
    -DartifactId=terajdbc4 \
    -Dversion=16.20.00.10 \
    -Dpackaging=jar \
    -Dfile=$STITCHR_ROOT/jars/terajdbc4.jar \
    -DgeneratePom=true

mvn install:install-file \
    -DgroupId=com.teradata.jdbc \
    -DartifactId=tdgssconfig \
    -Dversion=16.20.00.10 \
    -Dpackaging=jar \
    -Dfile=$STITCHR_ROOT/jars/tdgssconfig.jar \
    -DgeneratePom=true


