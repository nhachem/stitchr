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

import com.stitchr.util.SharedSession.spark
import com.stitchr.app.DerivationService
import com.stitchr.app.DataMoveService.instantiateQueryList
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
// For postgres tests, Expects the tpcds schema is deployed
/* persistence  id = 3 is file system,  pipe delimited
 and 1 is for postgres tpcds
 */
// files
val ql0 = List("q2_3","q4_3")

// database based example
// q21 is the same as q2 in the registry but associated with a database schema
val ql1 = List("q21_1")

val ds = new DerivationService

println("start derivation")
ds.deriveQueryList(ql0)
ds.deriveQueryList(ql1)

// println("start running queries")
// runQueries (ql0, stFile)
// runQueries (ql1, stDatabase)

infoListTables()

/* persistence  id = 3 is file system,  pipe delimited
 and 1 is for postgres tpcds
 */
spark.sql("select * from q2_3").show(50)
spark.sql("select * from q21_1").show(50)
spark.sql("select * from q4_3").show(50)

val q21DF = spark.table("q21_1")
q21DF.show(10, truncate = false)

//import spark.sqlContext.implicits._
//spark.sparkContext.emptyRDD.toDF()

// change logging to warn
infoListTables()

// DataIngestService
instantiateQueryList(ql0)

logging.log.info("done with q2 and q4")
// DataIngestService.
instantiateQueryList(ql1)
logging.log.info("done with q21")

// store in data lake
print(s"storing web_sales in the data lake ")
// adding web_sales as a direct example
// NH: 12/8/20... deprecating... materializing base object as copies is not to be supported directly.
// in upcoming changes new datasets will act as the copies
val (viewName, dfm) = getDataSet(getObjectRef("web_sales", "file")).materialize
val (viewName3, dfm3) = getDataSet("web_sales_3").materialize
val (viewName1, dfm1) = getDataSet("web_sales_1").materialize

// show all tables assumes applogLevel = INFO
infoListTables()


