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

import com.stitchr.util.EnvConfig.{ baseRegistryFolder, props, dataCatalogPersistence }
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.util.Convert.config2JdbcProp
import com.stitchr.sparkutil.SharedSession.spark
object RegistrySchema {

  import org.apache.spark.sql.types._

  /**
   * schema_columns.csv looks
   * [object_ref,column_name,ordinal_position,data_type,numeric_precision,character_maximum_length,is_nullable]
   */
  // NH: need to use
  val schemaDef: StructType = new StructType()
    .add("name", StringType)
    .add("df_type", StringType)
    .add("engine", StringType)
    .add("dataset_container", StringType)
    .add("table_name", StringType)
    .add("storage", StringType)
    .add("relative_url", StringType)
    .add("description", StringType)

  // name,df_type,engine,dataset_container,table_name,storage,relative_url,description,data_source_index
  case class DatasetMeta(
      name: String,
      df_type: String,
      engine: String,
      dataset_container: String,
      table_name: String,
      storage: String,
      relative_url: String,
      description: String,
      data_source_index: String
  )

  /**
   *root (reworked)
   *|-- id: integer (nullable = false)
   *|-- object_ref: string (nullable = false)... to delete
   *|-- format: string (nullable = false)
   *|-- storage_type: string (nullable = false)
   *|-- mode: string (nullable = true)
   *|-- container: string (nullable = true)
   *|-- object_type: string (nullable = false)
   *|-- object_name: string (nullable = false)
   *|-- data_source_ref: string (nullable = true)
   *|-- schema_ref: string (nullable = true)
   *|-- query: string (nullable = true)
   *|-- partition_key: string (nullable = true)
   * parallelism ... NH: should be pushed to somewhere else... but ok for now
   *
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
   */
// may use later
  val datasetSchema = new StructType()
    .add("id", IntegerType)
    .add("object_ref", StringType)
    .add("format", StringType)
    .add("storage_type", StringType)
    .add("mode", StringType)
    .add("container", StringType)
    .add("object_type", StringType)
    .add("object_name", StringType)
    .add("data_source_ref", StringType)
    .add("schema_ref", StringType)
    .add("query", StringType)
    .add("partition_key", StringType)
    .add("number_partitions", IntegerType)
    .add("priority_level", IntegerType)
    .add("dataset_state_id", IntegerType)

  /* schemas column defs */
  /*    .add("object_ref", StringType)
    .add("column_name", StringType)
    .add("column_position", IntegerType)
    .add("column_type", StringType)
    .add("precision", IntegerType)
    .add("string_length", IntegerType)
    .add("is_nullable", BooleanType) */

  val schemasSchema = new StructType()
    .add("schema_id", IntegerType, false)
    .add("object_ref", StringType, false)
    .add("column_name", StringType, false)
    .add("column_position", IntegerType, false)
    .add("column_type", StringType, false)
    .add("precision", IntegerType, true) // need to fix null representation
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
  * to do do we wpuld use DS and not DF?! */
  val (datasetDF, schemasDF, dataSourceDF) = dataCatalogPersistence match {
    case "dc" => {
      val jdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
      (
          jdbc.readDF("select * from public.dataset").cache(),
          jdbc.readDF("select * from public.schema_column").cache(),
          jdbc.readDF("select * from public.data_source").cache()
      )
    }
    case "registry" => {
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

    }
    case _ => { // assumes default to jdbc
      val jdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
      (
          jdbc.readDF("select * from public.dataset").cache(),
          jdbc.readDF("select * from public.schema_column").cache(),
          jdbc.readDF("select * from public.data_source").cache()
      )
    }
  }

}
