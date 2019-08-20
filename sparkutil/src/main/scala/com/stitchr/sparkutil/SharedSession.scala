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

package com.stitchr.sparkutil

import com.stitchr.util.EnvConfig.{ hiveSupport, logLevel }
import com.stitchr.util.Properties.configHadoop

object SharedSession {

  import org.apache.spark.sql.SparkSession

  // used to setup hive supported session for now but want it to be used to reset a session
  def initSparkSession(hiveSupport: Boolean = false): SparkSession = {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    SparkSession.clearDefaultSession()

    // always session before context starting from Spark 2.3
    val ss =
      if (hiveSupport)
        SparkSession.builder
          .appName("Stitchr with hive support")
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("spark.sql.catalogImplementation", "hive")
          .config("spark.sql.warehouse.dir", warehouseLocation) // new File("spark-warehouse").getAbsolutePath())
          .config("spark.driver.allowMultipleContexts", "true")
          .enableHiveSupport
          .getOrCreate
      else
        SparkSession.builder
          .appName("Stitchr")
          // .master("local[*]")// avoid hardcoding the deployment environment
          // .config("spark.sql.warehouse.dir", warehouseLocation)
          // .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
          // .config("spark.driver.allowMultipleContexts", "true")
          // .config("spark.driver.allowMultipleContexts", value = true)
          .config("spark.sql.parquet.filterPushdown", value = true)
          .config("spark.sql.parquet.mergeSchema", value = false)
          .config("spark.speculation", value = false)
          .getOrCreate

    // we set up the hadoop config for the session here
    val _ = configHadoop()
    // set log level
    ss.sparkContext.setLogLevel(logLevel)
    ss
  }

  // setting spark configs... need to merge within the initialization
  def configSpark(): SparkSession =
    /* those are spark config add ons. Many may be actually defaults but we want to ensure that they are set */
    /* spark.sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")
      spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "100000")
    // lzo uncompressed snappy and gzip is default
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip") */
    SparkSession.builder
    //.appName("my app")  // optional and will be auto-generated if not specified
    // .master("local[*]")// avoid hardcoding the deployment environment
    // .enableHiveSupport() // self-explanatory, isn't it?
    // .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.speculation", value = false)
      //  .config("spark.driver.extraClassPath", "/home/hadoop/code/edap-poc/jars/...")
      //  .config("spark.executor.extraClassPath", "/home/hadoop/code/edap-poc/jars/...")
      .getOrCreate()

  /* this is shared everywhere as the spark session
  forcing to non hive for now */
  val spark: SparkSession = initSparkSession(hiveSupport)

}
