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

package com.stitchr.core.dbapi
import com.stitchr.util.database.JdbcProps

import org.apache.spark.sql.DataFrame

trait SparkJdbc {

  import java.util.Properties
  def jdbcProps: JdbcProps

  val url: String = setupUrl()
  val connectionProperties: Properties = setupConnectionProperties()

  def setupUrl(): String

  def setupConnectionProperties(): Properties

  def readDF(query: String): DataFrame

  // maybe to write as a DF extension instead?
  // default to no parallelism or base implementation
  def readDF(query: String, partitionKey: String, numberOfPartitions: Int): DataFrame =
    readDF(query: String)

  // untested
  def writeTable(dataframe: DataFrame, tableName: String, numberPartitions: Int, saveMode: String): Unit
}
