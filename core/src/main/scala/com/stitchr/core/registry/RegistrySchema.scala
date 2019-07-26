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

package com.stitchr.core.registry

import com.stitchr.core.common.Encoders.{ datasetEncoder, DataSet }
import com.stitchr.util.EnvConfig.{ baseRegistryFolder, dataCatalogPersistence, props }
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.util.Convert.config2JdbcProp
import com.stitchr.sparkutil.SharedSession.spark
// import com.stitchr.util.Logging

import org.apache.spark.sql.{ DataFrame, Dataset }

object RegistrySchema {

  import org.apache.spark.sql.types._

  /**
   * schema_columns.csv looks
   * [object_ref,column_name,ordinal_position,data_type,numeric_precision,character_maximum_length,is_nullable]
   */
  // NH: need to use?!
  val schemaDef: StructType = new StructType()
    .add("name", StringType)
    .add("df_type", StringType)
    .add("engine", StringType)
    .add("dataset_container", StringType)
    .add("table_name", StringType)
    .add("storage", StringType)
    .add("relative_url", StringType)
    .add("description", StringType)

  case class DatasetMeta(
      name: String,
      df_type: String,
      engine: String,
      dataset_container: String,
      table_name: String,
      storage: String,
      relative_url: String,
      description: String,
      data_source_id: String
  )

  /**
    case class DataSet(
                      id: Int,
                      object_ref: String,
                      format: String,
                      storage_type: String,
                      mode: String,
                      container: String,
                      object_type: String,
                      object_name: String,
                      schema_ref: String,
                      query: String,
                      partition_key: String,
                      number_partitions: Int,
                      priority_level: Int,
                      dataset_state_id: Int
                      schema_id: Int
                      data_source_id: Int
                      schema_id: Int
                    )
   */
// may use later
  val datasetSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("object_ref", StringType)
    .add("format", StringType)
    .add("storage_type", StringType)
    .add("mode", StringType)
    .add("container", StringType)
    .add("object_type", StringType)
    .add("object_name", StringType)
    .add("query", StringType)
    .add("partition_key", StringType)
    .add("number_partitions", IntegerType)
    .add("priority_level", IntegerType)
    .add("dataset_state_id", IntegerType)
    .add("schema_id", IntegerType)
    .add("data_source_id", IntegerType)
    .add("data_destination_id", IntegerType)

  /* schemas column defs */
  /*    .add("object_ref", StringType)
    .add("column_name", StringType)
    .add("column_position", IntegerType)
    .add("column_type", StringType)
    .add("column_precision", IntegerType)
    .add("string_length", IntegerType)
    .add("is_nullable", BooleanType) */

  val schemasSchema: StructType = new StructType()
    .add("id", IntegerType, false)
    //  .add("object_ref", StringType, false)
    .add("column_name", StringType, false)
    .add("column_position", IntegerType, false)
    .add("column_type", StringType, false)
    .add("column_precision", IntegerType, true) // need to fix null representation
    .add("string_length", IntegerType, true) // need to fix null representation
    .add("is_nullable", StringType, false)
  //.add("c_type", StringType)
  /* had problem decyphering nulls in integer fields and also boolean....
had to edit and replace nulls with -q for now and bypass the use of boolean ype
   */
  /* val schemasDF = spark.read
    .schema(schemasSchema)
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .load(baseFolder + "schema_column.csv")
    .cache() */

  //this has an issue working with nulls!!
  /*  val schemasDF = spark.read.format("csv")
    .option("header", true)
    .option("nullValue", "")
    .load(baseFolder + "schema_columns.csv").cache()
   */

  /* we should try to abstract further with a common api?!
   * to do we would use DS and not DF to enforce proper typing?! */
  /*
  NH: 7/26/2019. the schema_column.string_length is also used to capture the decimal precision of a numeric.
  We need to adjust the names of the attributes as the current naming is wrong and leads to confusion
   */
  val (datasetDF, schemasDF, dataSourceDF): (DataFrame, DataFrame, DataFrame) = dataCatalogPersistence match {
    case "dc" =>
      val jdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
      (
          jdbc
            .readDF(
                s"""select id,
               | object_ref,
               | format,
               | storage_type, mode,
               | container, object_type,
               | object_name, query, partition_key,
               | number_partitions, priority_level,
               | dataset_state_id,
               | schema_id,
               | data_source_id,
               | data_destination_id
               | from public.dataset""".stripMargin
            )
            .cache(),
          jdbc.readDF("select * from public.schema_column").cache(),
          jdbc.readDF("select * from public.data_source").cache()
      )
    case "registry" =>
      (
          spark.read
            .schema(datasetSchema)
            .format("csv")
            .option("header", true)
            .option("quote", "\"")
            .option("multiLine", true)
            .option("inferSchema", "false")
            .option("delimiter", ",")
            .load(baseRegistryFolder + "dataset.csv")
            .cache(),
          spark.read
            .schema(schemasSchema)
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("delimiter", ",")
            .load(baseRegistryFolder + "schema_column.csv")
            .cache(),
          spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .load(baseRegistryFolder + "data_source.csv")
            .cache()
      )
    case _ => // assumes default to jdbc
      val jdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
      (
          jdbc
            .readDF(
                s"""select id,
               | object_ref,
               | format,
               | storage_type, mode,
               | container, object_type,
               | object_name, query, partition_key,
               | number_partitions, priority_level,
               | dataset_state_id,
               | schema_id,
               | data_source_id,
               | data_destination_id
               | from public.dataset""".stripMargin
            )
            .cache(),
          jdbc
            .readDF(
                s"""select id, column_name,
               | column_position,
               | column_type,
               | column_precision,
               | string_length,
               | is_nullable
               | from public.schema_column""".stripMargin
            )
            .cache(),
          jdbc.readDF(s"""select *
               | from public.data_source""".stripMargin).cache()
      )
  }
// Dataset[Encoders.DataSet]
  val datasetDS: Dataset[DataSet] = datasetDF
    .select(
        "id",
        "object_ref", // NH: 7/10/2019... need to deprecate the use of object_ref...
        "format",
        "storage_type",
        "mode",
        "container",
        "object_type",
        "object_name",
        "query",
        "partition_key",
        "number_partitions",
        "priority_level",
        "dataset_state_id",
        "schema_id",
        "data_source_id",
        "data_destination_id"
    )
    .as(datasetEncoder)
}
