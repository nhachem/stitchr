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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoder

/**
 * Encoders are used to add stronger typing to the code and encourage Dataset vs DataFrame
 */
object Encoders {

  /**
   *
   * @param id
   * @param object_ref key to the DataSet
   * @param object_name is usnique within a data paersistence l;ayer (albeit within a container
   * @param query
   * @param mode
   * @param data_persistence_id
   */
  case class QueryNode(id: Int, object_ref: String, object_name: String, query: String, mode: String, data_persistence_id: Int)

  /**
   * Captures the metadata used to characterize a Stitchr DataSet
   * @param id
   * @param object_ref
   * @param format
   * @param storage_type
   * @param mode
   * @param container
   * @param object_type
   * @param object_name
   * @param query
   * @param partition_key
   * @param number_partitions
   * @param schema_id
   * @param data_persistence_src_id
   * @param data_persistence_dest_id
   * @param add_run_time_ref
   * @param write_mode
   */
  case class DataSet(
      id: Int,
      object_ref: String, // computed as obj
      format: String,
      storage_type: String,
      mode: String,
      container: String,
      object_type: String,
      object_name: String,
      query: String,
      partition_key: String,
      number_partitions: Int,
      schema_id: Int,
      data_persistence_src_id: Int,
      data_persistence_dest_id: Int = -1 // added to specify where we move an object -1 means don't move, 0 means temp space (should populate that in the data_persistence table)
      ,
      add_run_time_ref: Boolean = false,
      write_mode: String = "append"
  )

  // case class ExtendedDependency(object_name: String, depends_on: String, query: String, schema_id: Int = -1, data_persistence_id: Int = -1)
  /**
   * A Dependency relation captures lineage between (sets of) DatSets. It captures every From object in a sql query
   * @param object_name
   * @param depends_on
   * @param dataset_id
   * @param storage_type
   * @param query
   * @param schema_id
   * @param data_persistence_id
   */
  case class Dependency(object_name: String, depends_on: String, dataset_id: Int, storage_type: String, query: String, schema_id: Int, data_persistence_id: Int)

  /**
   *
   * @param position
   * @param name
   * @param att_type
   */
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

  /**
   * catptures the attributes characterizing a data persistence layer (persistence zone)
   * @param id
   * @param name
   * @param persistence_type
   * @param storage_type
   * @param driver
   * @param host
   * @param port
   * @param db
   * @param user
   * @param pwd
   * @param fetchsize
   * @param sslmode
   */
  case class DataPersistence(
      id: Int,
      name: String,
      persistence_type: String,
      storage_type: String,
      driver: String,
      host: String,
      port: Int,
      db: String,
      user: String = null,
      pwd: String = null,
      // index: String,
      fetchsize: Int,
      sslmode: String
  )

  // id, position are unique
  /**
   * id, column_position are unique
   * @param id
   * @param column_name
   * @param column_position
   * @param column_type
   * @param column_precision
   * @param string_length
   * @param is_nullable
   */
  case class SchemaColumn(
      id: Int,
      column_name: String,
      column_position: Int,
      column_type: String,
      column_precision: Int,
      string_length: Int,
      is_nullable: Boolean
  )

  /**
   * currently really unused but decided to keep around for now. It replaces ids with the instance objects they reference
   * schema, dataSourcePersistence and dataDestinationPersistence
   * @param id
   * @param format
   * @param storage_type
   * @param mode
   * @param container
   * @param object_type
   * @param object_name
   * @param query
   * @param partition_key
   * @param number_partitions
   * @param schema
   * @param dataSourcePersistence
   * @param dataDestinationPersistence
   */
  case class ExtendedDataSet(
      id: Int,
      format: String,
      storage_type: String,
      mode: String,
      container: String,
      object_type: String,
      object_name: String,
      query: String,
      partition_key: String,
      number_partitions: Int,
      schema: StructType, // = getSchema(schema_id),
      dataSourcePersistence: DataPersistence,
      dataDestinationPersistence: DataPersistence // added to specify where we move an object -1 means don't move, 0 means temp space (should populate that in the data_persistence table)
  )

  // encoders...
  val dataSetEncoder: Encoder[DataSet] = org.apache.spark.sql.Encoders.product[DataSet]
  // val extendedDataSetEncoder: Encoder[ExtendedDataSet] = org.apache.spark.sql.Encoders.product[ExtendedDataSet]
  val queryNodeEncoder: Encoder[QueryNode] = org.apache.spark.sql.Encoders.product[QueryNode]
  val dependencyEncoder: Encoder[Dependency] = org.apache.spark.sql.Encoders.product[Dependency]
  val schemaColumnEncoder: Encoder[SchemaColumn] = org.apache.spark.sql.Encoders.product[SchemaColumn]
  val dataPersistenceEncoder: Encoder[DataPersistence] = org.apache.spark.sql.Encoders.product[DataPersistence]

  // empty structures
  val emptyDs = new DataSet(-1, "EmptyDataSet_0", "", "", "", "", "", "EmptyDataSet", "", "", -1, -1, 0, -1)
  val emptyDp: DataPersistence = new DataPersistence(-1, "EmptyDataPersistence", "", "", "", "", -1, "", "", "", -1, "prefer")
  val emptyDependency: Dependency = Dependency(null, null, null.asInstanceOf[Int], null, null, null.asInstanceOf[Int], null.asInstanceOf[Int])
}
