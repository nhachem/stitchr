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

"""
File: Stitchr.py
This module will be used for holding all functions which relate to invoking stitchr from pyspark

to run interactively using pyspark:
in the directory where the run_demo.py is located run

pyspark --jars $STITCHR_ROOT/app/target/stitchr-app-0.1-SNAPSHOT-jar-with-dependencies.jar

then at the prompt type

import run_demo

Version: 0.1:
    - testing
"""

import pyspark
from pyspark.sql import SparkSession


class Stitchr:
    def __init__(self, spark_session, spark_context):
        """ Setting up defaults and holders until real values are known. """
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.jvm = self.spark_context._jvm
        self.jcatalog = spark_session._jsparkSession.catalog()

    def derive_query(self, query):
        return self.jvm.com.stitchr.core.dataflow.Runner.run(query)


