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

package com.stitchr.core.common

import com.stitchr.util.database.JdbcProps
import org.apache.spark.sql.Encoder

object Encoders {
  /* NH: 7/1/2019 will need to consolidate the classes */
  case class QueryNode(object_name: String, query: String, mode: String)
  // will need to rename maybe use DataSet
  case class DataSet(
      id: Int,
      object_ref: String,
      format: String,
      storage_type: String,
      mode: String,
      container: String,
      object_type: String,
      object_name: String,
      data_source_ref: String,
      schema_ref: String,
      query: String,
      partition_key: String,
      number_partitions: Int,
      priority_level: Int,
      dataset_state_id: Int
  )
  case class Dependency(object_name: String, depends_on: String)
  case class ExtendedDependency(object_name: String, depends_on: String, query: String, data_source_ref: String)
  case class Column(position: Int, name: String, att_type: String)

  // jdbc: <sourceType>:<storageType>://<host>:<port>/<database> + user and password from system for now)
  /* case class JdbcProps(
      dbms: String,
      driver: String,
      host: String,
      port: Int,
      database: String,
      dbIndex: String = "jdbc",
      user: String = null,
      pwd: String = null,
      fetchsize: Int = 10000
  ) */

  // NH: 6/28/2019. need to adjust and rename columns if necessary
  case class DataSource(
      id: Int,
      source_type: String,
      storage_type: String,
      driver: String,
      host: String,
      port: Int,
      database: String,
      user: String = null,
      pwd: String = null,
      index: String,
      fetchsize: Int
  )
  case class SchemaColumn(
      schema_id: String,
      object_ref: String,
      name: String,
      position: Int,
      att_type: String,
      column_precision: Int,
      string_length: Int,
      is_nullable: Boolean
  )

  // encoders...
  val datasetEncoder: Encoder[DataSet] = org.apache.spark.sql.Encoders.product[DataSet]
  val queryNodeEncoder: Encoder[QueryNode] = org.apache.spark.sql.Encoders.product[QueryNode]
  val dependencyEncoder: Encoder[Dependency] = org.apache.spark.sql.Encoders.product[Dependency]
  val extendedDependencyEncoder: Encoder[ExtendedDependency] = org.apache.spark.sql.Encoders.product[ExtendedDependency]
  val columnEncoder: Encoder[Column] = org.apache.spark.sql.Encoders.product[Column]
  val dataSourceEncoder: Encoder[DataSource] = org.apache.spark.sql.Encoders.product[DataSource]

}
