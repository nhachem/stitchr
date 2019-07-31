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
// spark-shell --jars $STITCHR_ROOT/app/target/stitchr-app-0.1-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.11:2.4.3

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
import com.stitchr.app.{DataIngestService, DerivationService, Derivation}
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.dataflow.ComputeService.runQueries
import com.stitchr.core.registry.RegistrySchema.{dataSourceDF, datasetDF}
import com.stitchr.core.registry.RegistryService.{getDataSource, getDataset, getObjectRef}
import com.stitchr.core.util.Convert.dataSourceNode2JdbcProp
import com.stitchr.util.Logging
import com.stitchr.util.Properties.configS3
import com.stitchr.util.Util.time
import com.stitchr.core.api.DataSet.Implicits

val logging = new Logging

spark.sparkContext.setLogLevel("INFO")

// just list the session info
val configMap:Map[String, String] = spark.conf.getAll

/**
  * edit the parameters below to go against a target dbms or files. By default we run q2 on files (on yr laptop)
  */
// For postgres tests, Expects the tpcds schema is deployed
// file based example
val stFile = "file"
val ql0 = List("q2","q4")
//val ql0 = List("web_sales_m")

Derivation.run(ql0)

// database based example
// q21 is the same as q2 in the registry but associated with a database schema
val ql1 = List("q21")
// val ql1 = List("web_sales_m")
val stDatabase = "database"

val ds = new DerivationService

ds.deriveQueryList(ql0) // ,stFile)
runQueries (ql0, stFile)

ds.deriveQueryList(ql1) // ,stDatabase)
runQueries (ql1, stDatabase)

// clear all catalog cache for reruns with updated data catalog files
spark.sql("clear cache").show()

spark.catalog.listTables.show(50, false)


// temporary out to speed up dev
spark.sql("select * from q2").show(50)
spark.sql("select * from postgresql_1_q21").show(50)
spark.sql("select * from q4").show(50)

val q21DF = spark.table("postgresql_1_q21")
q21DF.show(50,false)

// NH: 7/11/2019 ... need to add the write use cases. ephemeral to tmp and persistence to target container using data sources
// import spark.sqlContext.implicits._
// spark.sparkContext.emptyRDD.toDF()
// change logging to warn
spark.catalog.listTables.show(50, false)

def instantiateQueryList(ql: List[String], dataType: String = "database"): Unit = { // , st: String = "file"): Unit = {

  val _ = configS3() // needed for AWS ... may make conditional based on config or metadata

  // assumed to be not needed
  // val ds = new DerivationService
  // ds.deriveQueryList(ql)

  // Then run the actual materialization of the target objects
  ql.foldLeft()(
    (_, next) => {
      println(s"instantiating the target for $next")
      val (viewName, dfm) = getDataset(getObjectRef(next, dataType)).materialize
      println(s"viewname is $viewName")
      spark.sparkContext.setLogLevel("INFO")
      time(dfm.count, "counting the rows in the materialized object")
      dfm.printSchema()

    }
  )
}
spark.sparkContext.setLogLevel("WARN")
// DataIngestService.
  instantiateQueryList(ql0, "file")
// DataIngestService.
  instantiateQueryList(ql1, "database")

/*
spark.sparkContext.setLogLevel("WARN")
import com.stitchr.core.api.Helpers._
import com.stitchr.core.registry.RegistryService._



val (viewName, dfm) = getDataset("q2").materialize
println(s"viewname is $viewName")
spark.sparkContext.setLogLevel("INFO")
time(dfm.count, "counting the rows in the materialized object")
dfm.printSchema()

val (viewName1, dfm1) = getDataset("postgresql_1_q21").materialize
println(s"viewname is $viewName1")
time(dfm1.count, "counting the rows in the materialized object")
dfm1.printSchema()
*/
// adding web_sales as a direct example
val (viewName, dfm) = getDataset(getObjectRef("web_sales", "file")).materialize

// adding web_sales in the db as an exaple (of write to DB)

// show all
spark.catalog.listTables.show(false)
// show only the datalake contents
spark.catalog.listTables.filter("name like 'datalake%'").show(false)
