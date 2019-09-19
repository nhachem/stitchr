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

import com.stitchr.core.common.Encoders.{ DataSet, dataSetEncoder }
import com.stitchr.util.EnvConfig.{ baseRegistryFolder, dataCatalogPersistence, props }
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.util.Convert.config2JdbcProp
import com.stitchr.sparkutil.SharedSession.spark
import org.apache.spark.sql.{ DataFrame, Dataset }

object RegistrySchema {

  import org.apache.spark.sql.types._

  /**
   * schema_column.csv looks
   * [id, column_name,ordinal_position,data_type,numeric_precision,character_maximum_length,is_nullable]
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
      data_persistence_src_id: String
  )

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
    .add("schema_id", IntegerType)
    .add("data_persistence_src_id", IntegerType)
    .add("data_persistence_dest_id", IntegerType)

  /* schemas column defs
     .add("id", IntegerType)
    .add("column_name", StringType)
    .add("column_position", IntegerType)
    .add("column_type", StringType)
    .add("column_precision", IntegerType)
    .add("string_length", IntegerType)
    .add("is_nullable", BooleanType)
   */
  val schemasSchema: StructType = new StructType()
    .add("id", IntegerType, false)
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
  val (dataSetDF, schemasDF, dataPersistenceDF, batchGroupDF, batchGroupMembersDF): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) =
    dataCatalogPersistence match {
      case "dc" =>
        val jdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
        (
            jdbc
              .readDF(
                  s"""select id,
               | CONCAT(object_name, '_', data_persistence_src_id) as object_ref,
               | format,
               | storage_type, mode,
               | container, object_type,
               | object_name, query,
               | partition_key,
               | number_partitions,
               | schema_id,
               | data_persistence_src_id,
               | data_persistence_dest_id
               | from public.dataset""".stripMargin
              )
              .cache(),
            jdbc
              .readDF(
                  s"""select id,
               | column_name,
               | column_position,
               | column_type,
               | column_precision,
               | string_length,
               | is_nullable
               | from public.schema_column""".stripMargin
              )
              .cache(),
            jdbc
              .readDF(
                  s"""select id,
                | name,
                | persistence_type,
                | storage_type ,
                | host,
                | port,
                | db,
                | "user",
                | pwd,
                | driver,
                | fetchsize
                | from public.data_persistence""".stripMargin
              )
              .cache(),
            jdbc.readDF(
                s"""select
                   | g.id,
               | g.name
               | from batch_group g
               | """.stripMargin
            ),
            jdbc.readDF(
                s"""select
                   | bgm.group_id,
               | bgm.dataset_id
               | from
               | batch_group_members bgm
               | """.stripMargin
            )
            // better to read the tables in and join with  datsetdf?!
            /*  jdbc.readDF(
                s"""select bgm.group_id,
                     | g.name,
                     | bgm.dataset_id,
                     | d.object_name || '_' || data_persistence_src_id as object_ref
                     | from
                     | batch_group g,
                     | batch_group_members bgm,
                     | dataset d
                     | where d.id = bgm.dataset_id
                     | and g.id = bgm.group_id
                     | """.stripMargin
            ) */
        )
      // to fix to use data_persistence
      case "registry" =>
        ( {
          import org.apache.spark.sql.functions._ // {concat, lit, }
          val df =
            spark.read
              // .schema(datasetSchema)
              .format("csv")
              .option("header", true)
              .option("quote", "\"")
              .option("multiLine", true)
              .option("inferSchema", "true")
              .option("delimiter", ",")
              .load(baseRegistryFolder + "dataset.csv")
              .select( "id",
                "format",
                "storage_type",
                "mode",
                "container",
                "object_type",
                "object_name",
                "query",
                "partition_key",
                "number_partitions",
                "schema_id",
                "data_persistence_src_id",
                "data_persistence_dest_id"
              )
          // convoluted but fine for now... maybe better to use cast straight in the select above?
           df.withColumn("id_", df.col("id")
             .cast(IntegerType)).drop("id")
             .withColumnRenamed("id_", "id")
             .withColumn ("object_ref", concat (df.col ("object_name") , lit ("_"), df.col ("data_persistence_src_id")))
             .cache()
        },
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
              .load(baseRegistryFolder + "data_persistence.csv")
              .cache(),
            // NH: maybe will add in V0.2 but files are only for demo purposes and are not transactional
            // this is a placeholder for now...
            spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", ",")
              .load(baseRegistryFolder + "batch_group.csv")
              .cache(),
            spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", ",")
              .load(baseRegistryFolder + "batch_group_members.csv")
              .cache()
        )
      case _ => // assumes default to jdbc
        val jdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
        (
            jdbc
              .readDF(
                  s"""select id,
               | CONCAT(object_name, '_', data_persistence_src_id) as object_ref,
               | format,
               | storage_type,
               | mode,
               | container,
               | object_type,
               | object_name,
               | query,
               | partition_key,
               | number_partitions,
               | schema_id,
               | data_persistence_src_id,
               | data_persistence_dest_id
               | from public.dataset""".stripMargin
              )
              .cache(),
            jdbc
              .readDF(
                  s"""select id,
                   | column_name,
                   | column_position,
               | column_type,
               | column_precision,
               | string_length,
               | is_nullable
               | from public.schema_column""".stripMargin
              )
              .cache(),
            jdbc
              .readDF(
                  s"""select id,
                         | name,
                         | persistence_type,
                         | storage_type ,
                         | host,
                         | port,
                         | db,
                         | "user",
                         | pwd,
                         | driver,
                         | fetchsize
                         | from public.data_persistence""".stripMargin
              )
              .cache(),
            jdbc.readDF(
                s"""select
                   | g.id,
               | g.name
               | from batch_group g
               | """.stripMargin
            ),
            jdbc.readDF(
                s"""select
                   | bgm.group_id,
             | bgm.dataset_id
             | from
             | batch_group_members bgm
             | """.stripMargin
            )
        )
    }
// Dataset[Encoders.DataSet]
  val dataSetDS: Dataset[DataSet] = dataSetDF
    .select(
        "id",
        "object_ref",
        "format",
        "storage_type",
        "mode",
        "container",
        "object_type",
        "object_name",
        "query",
        "partition_key",
        "number_partitions",
        "schema_id",
        "data_persistence_src_id",
        "data_persistence_dest_id"
    )
    .as(dataSetEncoder)

  val groupListDF = batchGroupDF
    .join(batchGroupMembersDF, batchGroupDF.col("id") === batchGroupMembersDF.col("group_id"))
    .join(dataSetDF, dataSetDF.col("id") === batchGroupMembersDF.col("dataset_id"))
    .select("group_id", "name", "dataset_id", "object_name", "data_persistence_src_id")
  //val extendedDataSetDs: Dataset[ExtendedDataSet] =
  //  dataSetDS.map{ r: DataSet => extendedFromDataSet(r) }

}
