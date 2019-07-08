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
note that the variables are local to ex
so to run a new derivation one would need to use run_demo.s....

to run interactively using pyspark:
in the directory where the run_demo.py is located run

pyspark --jars $STITCHR_ROOT/core/target/stitchr-core-0.1-SNAPSHOT-jar-with-dependencies.jar

then at the prompt type

import run_demo

"""
import pyspark
from pyspark.sql import SparkSession

import os
import sys

""" setting up the path to include Stitchr and other project related imports"""
sys.path.append(os.environ['STITCHR_ROOT'] + '/pyspark-app/app')

from Stitchr import *

spark = (pyspark.sql.SparkSession.builder.getOrCreate())
spark.sparkContext.setLogLevel('WARN')

print("Spark Version:", spark.sparkContext.version)

s = Stitchr(spark, spark.sparkContext)

s.derive_query('q2', 'file')

s.derive_query('q4', 'file')


spark.catalog.listTables()

df = spark.sql("select * from q2").cache()
df.show(50)
df = spark.sql("select * from q4").cache()
df.show(50)
