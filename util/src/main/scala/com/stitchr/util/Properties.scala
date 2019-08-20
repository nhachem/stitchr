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

package com.stitchr.util

import java.io.File

import com.stitchr.util.EnvConfig.cloudStorage
import com.typesafe.config.{Config, ConfigFactory}

object Properties {

  import org.apache.hadoop.conf.Configuration
  import org.apache.spark.sql.SparkSession

  // need to fix and test for config directory...
  def readConfig(configurationFile: String = "defaults.properties", configRootPath: String = "config/"): Config = {
    val configFilePath = configRootPath
    val commonConfigFile = configurationFile
    val configFile = new File(configRootPath + configurationFile)
    println("Configuration file " + configFile.getAbsolutePath)
    ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.parseFile(new File(configFilePath + commonConfigFile)))
  }

  def _configHadoop(): Configuration = {
    // example of config
    // import java.util.Map.Entry

    import org.apache.hadoop.conf.Configuration

    // import scala.collection.JavaConversions._

    val spark: SparkSession = SparkSession.builder.getOrCreate
    val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
    /* I am putting both s3 and s3a basic config here for testing.
     * on EMR and if using EMRFS then access with s3:// else s3a://
     * there are a lot more parameters we can play with but not urgent
     */
    /* not used
   val awsAccessKeyId = sys.env.get("awsAccessKeyId").get
   val awsSecretAccessKey = sys.env.get("awsSecretAccessKey").get
    hadoopConfig.set("fs.s3.awsAccesskeyId", awsAccessKeyId)
    hadoopConfig.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey)
    hadoopConfig.set("fs.s3a.access.key", awsAccessKeyId)
    hadoopConfig.set("fs.s3a.secret.key", awsSecretAccessKey)
     */
    hadoopConfig.set("fs.s3.server-side-encryption-algorithm", "AES256")
    hadoopConfig.set("fs.s3.enableServerSideEncryption", "true")
    hadoopConfig.set("fs.s3a.server-side-encryption-algorithm", "AES256")

    val pattern = "([0-9]{1}).([0-9]{1}).([0-9]{1})".r
    val pattern(major, _, _) = spark.sparkContext.version
    if (major.toInt > 1) {
      // for spark 2.0? use
      hadoopConfig.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    } else {
      // disabled in spark 2.0. works well for 1.6...
      // https://issues.apache.org/jira/browse/SPARK-10063
      // use in spark 1.6 but can lead to corruption if there a failure during writes
      hadoopConfig.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    }
    hadoopConfig
  }

  def configHadoop(): Configuration =  if (cloudStorage) _configHadoop() else null.asInstanceOf[Configuration]
}
