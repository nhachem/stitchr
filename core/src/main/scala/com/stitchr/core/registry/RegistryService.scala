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

import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.registry.RegistrySchema._
import com.stitchr.sparkutil.database.Schema.toSqlType
import com.stitchr.core.common.Encoders._
import com.stitchr.util.Logging
import org.apache.spark.sql.DataFrame

object RegistryService {

  import org.apache.spark.sql.types._
  import spark.implicits._

  val logging = new Logging

  // we will modify to return reference to dataframes if necessary but also could just assume theviews and use spark sql....
  // this is purely based on a registry through files.... we need to make it portable (file/s3/hdfs...)
  // 2 files are dataset.csv and schema_columns.csv
  // we provide a default for files.... but also can pick a jdbc db connection alternatively. data would not be mixed
  def initializeDataCatalogViews(): Unit = {

    datasetDF.createOrReplaceTempView("dc_datasets")
    schemasDF.createOrReplaceTempView("dc_schema_columns")
    dataSourceDF.createOrReplaceTempView("dc_data_source")
  }

  /**
   * input dataframe has a schema like
   * example
   * [vertica0,vertica,com.vertica.jdbc.Driver,host,80,dbname,user,xx]
   *
   */
  /* deprecating
  def getDataSourcePropertyRow(dataFrame: DataFrame, indexLookup: String): org.apache.spark.sql.Row = {
    // get the row for the data source properties
    val dataSourceProps: org.apache.spark.sql.Row = dataFrame.filter("index = '" + indexLookup + "'").collectAsList().get(0)
    dataSourceProps
  }
   */

  /**
   *
   *
   */
  def getDataSource(dataFrame: DataFrame, idLookup: Integer): com.stitchr.core.common.Encoders.DataSource = {
    // get the row for the data source properties as a DataSourceNode
    val dataSourceProps = dataFrame
      .filter("id = '" + idLookup + "'")
      .select("id", "source_type", "storage_type", "driver", "host", "port", "db", "user", "pwd", "fetchsize")
      .as(dataSourceEncoder)
      .collectAsList()
      .get(0)
    dataSourceProps
  }

  /**
   * schema_columns.csv looks
   * [id, object_ref,column_name,ordinal_position,data_type,numeric_precision,character_maximum_length,is_nullable]
   * deprecate object_ref and use id
   */
  def getSchema(schemaId: Integer): StructType = {

    val schemaMap =
      schemasDF.filter(s"id = '$schemaId' ").orderBy("column_position").select("column_name", "column_type", "column_precision", "string_length")

    // test first if we have a schemaMap by for example counting */
    if (schemaMap.count == 0) { return null } // return a null schema value if the query returns nothing  else proceed
    // NH: BUG?! not sure why this is failing.
    // val sm = schemaMap.map(r => (r("column_name").toString, r("column_type").toString, r("column_precision").asInstanceOf[Int], r("character_length").asInstanceOf[Int]))
    val sm = schemaMap.map(r => (r(0).toString, r(1).toString, r(2).asInstanceOf[Int], r(3).asInstanceOf[Int]))

    // this works but does not reflect complete schema
    val schema: StructType = StructType(
        sm.collect()
          .map({ s =>
            StructField(s._1, toSqlType(s._2, s._3, s._4).dataType, toSqlType(s._2, s._3, s._4).nullable)
          })
    )
    schema
  }

  // NH: 7/26/2019. We need to merge the 2
  import spark.implicits._
  def getObjectRef(objectRef: String): String =
    datasetDF
      .filter(s"object_ref = '$objectRef'")
      .select("format", "data_source_id", "object_name")
      .map { r =>
        s"${r(0)}_${r(1)}_${r(2)}"
      }
      .take(1)(0)

  def getObjectRef(objectName: String, objectType: String = "database"): String =
    objectType match {
      case "database" =>
        datasetDF
          .filter(s"object_name = '$objectName'")
          .select("format", "data_source_id", "object_name")
          .map { r =>
            s"${r(0)}_${r(1)}_${r(2)}"
          }
          .take(1)(0)
      case _ => objectName
    }

  /**
   * getDataSet takes an object_ref and refers the first DataSet reference object (we need to make sure we have uniqueness
   * @param objectRef
   * @return
   */
  def getDataset(objectRef: String): DataSet = {

    if (datasetDS.filter(r => r.object_ref == objectRef).count() > 1)
      logging.log.warn("number of dataset rows returned is greater than 1 and is " + datasetDS.filter(r => r.object_ref == objectRef).count().toString)

    datasetDS.filter(r => r.object_ref == objectRef).take(1)(0) // assumes one row back... need to out validation tests for dups
  }
  /* stubs for api calls
  so those would be to register a new dataset and its schema
   */
  def addDataset( /* some stuff to pass in */ ): String =
    "datasetID"
  /* to add the schema just pass it and transform in a set  (field_position, field_name, type, nullable) array of tuples that are added to the catalog */
  def addDatasetSchema(datasetId: String, DatasetSchema: StructType): Boolean =
    /* return true false */
    true

  /* end registry service */
}
