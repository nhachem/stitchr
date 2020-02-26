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

package com.stitchr.util.database

// import org.slf4j.{Logger, LoggerFactory}
// jdbc: <sourceType>:<storageType>://<host>:<port>/<database> + user and password from system for now)
case class JdbcProps(
    dbms: String,
    driver: String,
    host: String,
    port: Int,
    database: String,
    dbIndex: String = "jdbc",
    user: String = null,
    pwd: String = null,
    fetchsize: Int = 10000,
    sslmode: String = "prefer",
    db_scope: String = "open"
)

trait Jdbc {

  import java.util.Properties
  def jdbcProps: JdbcProps

  /*
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
   */

  val url: String = setupUrl()
  val connectionProperties: Properties = setupConnectionProperties()

  def setupUrl(): String

  def setupConnectionProperties(): Properties
}
