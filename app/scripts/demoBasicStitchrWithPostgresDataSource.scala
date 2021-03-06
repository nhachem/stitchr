/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// make sure you have the env variables set to your environment in env.sh and that env.sh is sourced
// we use avro as a default for materialization and need to add the package
//

/**
  * Used to demo use cases
  * 1. Source and Target are a databases
  * 2. Source and Target are Spark files and Spark warehouse
  *
  * The data is based on tpcds.
  * data files are stripped from the last | to make them directly readable and are stored under demo/data/tpcds
  * generate the data using tpcds tools and place in that directory before the demo.
  *
  * Avro  before 2.4 is part of the distribution
  * spark-shell --jars $STITCHR_ROOT/app/target/stitchr-app-$VERSION-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.11:2.4.3
  *
  * You can find a copy ready to use under nhachem/stitchr-demo
  */
// --conf spark.sql.hive.metastore.version=2.3.7
import com.stitchr.util.SharedSession.spark
import com.stitchr.app.DerivationService
import com.stitchr.app.DataTransformService.instantiateQueryList
import com.stitchr.core.registry.RegistryService.{getDataSet, getObjectRef}
import com.stitchr.util.EnvConfig.logging
import com.stitchr.core.api.DataSetApi.Implicits
import com.stitchr.util.database.CatalogUtil.infoListTables

spark.sparkContext.setLogLevel("WARN")

// just list the session info
val configMap:Map[String, String] = spark.conf.getAll

/**
  * edit the parameters below to go against a target dbms or files. By default we run q2 on files (on yr laptop)
  */

// database based example
// q21 is the same as q2 in the registry but associated with a database schema
val ql1 = List("postgresql1__public__q21")

val ds = new DerivationService

println("start derivation")

ds.deriveQueryList(ql1)

infoListTables()

spark.sql("select * from postgresql1__public__q21").show(50)

val q21DF = spark.table("postgresql1__public__q21")
q21DF.show(10, truncate = false)

infoListTables()



