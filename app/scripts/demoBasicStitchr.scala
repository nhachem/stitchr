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
// spark-shell --jars $STITCHR_ROOT/app/target/stitchr-app-0.1-SNAPSHOT-jar-with-dependencies.jar

/**
  * Used to demo use cases
  * 1. Source and Target are a databases
  * 2. Source and Target are Spark files and Spark warehouse
  *
  * The data is based on tpcds.
  * data files are stripped from the last | to make them directly readable and are stored under demo/data/tpcds
  * generate the data using tpcds tools and place in that directory before the demo.
  * You can find a copy ready to use under nhachem/stitchr-demo
  */

import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.app.DerivationService

// just list the session info
val configMap:Map[String, String] = spark.conf.getAll
// println(configMap)

/**
  * edit the parameters below to go against a target dbms or files. By default we run q2 on files (on yr laptop)
  */
// For postgres tests, Expects the tpcds schema is deployed
// file based example
val stFile = "file"
val ql0 = List("q2","q4")
//val ql0 = List("web_sales_m")

// database based example
// q21 is the same as q2 in the registry but associated with a database schema
val ql1 = List("q21")
// val ql1 = List("web_sales_m")
val stDatabase = "database"

val ds = new DerivationService

ds.deriveQueryList(ql0,stFile)

ds.deriveQueryList(ql1,stDatabase)


// clear all catalog cache for reruns with updated data catalog files
spark.sql("clear cache").show()

spark.catalog.listTables.show(50, false)


spark.sql("select * from q2").show(50)
spark.sql("select * from postgres0_q21").show(50)
spark.sql("select * from q4").show(50)

val q21DF = spark.table("postgres0_q21")
q21DF.show(false)

//import com.stitchr.core.registry.RegistrySchema._
// import com.stitchr.core.registry.RegistryService._
// getDataSource(dataSourceDF, "postgres0")

