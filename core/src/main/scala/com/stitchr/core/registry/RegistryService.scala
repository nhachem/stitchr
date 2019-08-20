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

import com.stitchr.core.api.ExtendedDataframe.DataFrameImplicits
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.registry.RegistrySchema._
import com.stitchr.sparkutil.database.Schema.toSqlType
import com.stitchr.core.common.Encoders.{ DataPersistence, DataSet, SchemaColumn, dataPersistenceEncoder, ExtendedDataSet, emptyDp, emptyDs }
import com.stitchr.util.EnvConfig.logging

import com.stitchr.util.JsonConverter._

import org.apache.spark.sql.Dataset

object RegistryService {

  import org.apache.spark.sql.types._
  import spark.implicits._

  // we need to move the DF/DS code from the schema registry to here?

  // we will modify to return reference to dataframes if necessary but also could just assume theviews and use spark sql....
  // this is purely based on a registry through files.... we need to make it portable (file/s3/hdfs...)
  // 2 files are dataset.csv and schema_columns.csv
  // we provide a default for files.... but also can pick a jdbc db connection alternatively. data would not be mixed
  def initializeDataCatalogViews(): Unit = {
    dataSetDF.createTemporaryView("dc_datasets")
    schemasDF.createTemporaryView("dc_schema_columns")
    dataPersistenceDF.createTemporaryView("dc_data_persistence")
    batchGroupDF.createTemporaryView("dc_batch_group")
    batchGroupMembersDF.createTemporaryView("dc_batch_group_members")
  }

  // insert/update a DataCatalog object

  import com.stitchr.util.database._
  import com.stitchr.util.EnvConfig.props
  import com.stitchr.core.util.Convert.config2JdbcProp

  /**
   * general space
   *
   */

  /**
   *
   * @param groupName
   * @return the list of dataset objects that need to be moves
   */
  def getQueryReferenceList(groupName: String): List[String] =
    groupListDF
      .filter(s"name = '$groupName'")
      .select("object_name", "data_persistence_src_id")
      .map { r =>
        s"${r(0)}_${r(1)}"
      }
      .collect
      .toList

  // implement a sequence or unique ID
  def getUID: String =
    // TODO
    "XXXYYYZZZ"

  def getSequenceId: Long =
    // TODO
    -11111

  // NH unused but would be useful as an abstraction
  def add2Dc[T](t: T): Unit = {
    val jdbc = JdbcImpl(config2JdbcProp(props, "dc"))
    t match {
      case p: DataSet         => // upsert ot dataset
      case p: DataPersistence => // upsert to data_persistence
      case _                  => logging.log.warn(s"unsupported type yet")
    }
  }

  /**
   * DataSet space
   */
  // NH: 7/26/2019. We need to merge the 2
  import spark.implicits._

  /**
   *
   * @param dsAlternateKey is object_name, data_persistence_src_id
   * @return
   */
  def getObjectRef(dsAlternateKey: (String, Int)): String =
    dataSetDF
      .filter(s"object_name = '${dsAlternateKey._1}' and data_persistence_src_id = '${dsAlternateKey._2}")
      .select("format", "data_persistence_src_id", "object_name")
      .map { r =>
        s"${r(0)}_${r(1)}_${r(2)}"
      }
      .take(1)(0)

  // NH: weak as it assumes we have unique names based on stirage types... which is not necessaruly true... Will deprecate in V0.2
  def getObjectRef(objectName: String, storageType: String = "database"): String =
    dataSetDF
      .filter(s"object_name = '${objectName}' and storage_type = '${storageType}'")
      .select("object_ref")
      .map { r =>
        s"${r(0)}"
      }
      .take(1)(0)

  /**
   * getDataSet takes an object_ref and refers the first DataSet reference object (we need to make sure we have uniqueness
   *
   * @param objectRef
   * @return
   */
  //NH: 8/9/2019 can make those implicit?? filter is applied based on input and then  everything else works the same
  def getDataSet(objectRef: String): DataSet = {
    val ds = dataSetDS.filter(r => r.object_ref == objectRef)

    if (dataSetDS.filter(r => r.object_ref == objectRef).count() > 1)
      logging.log.warn("number of dataset rows returned is greater than 1 and is " + dataSetDS.filter(r => r.object_ref == objectRef).count().toString)

    if (ds.count >= 1)
      ds.take(1)(0) // assumes one row back... need to out validation tests for dups
    else {
      // if you don't find a DS then return a default DS with id = -1...
      logging.log.warn("no dataset found returning the empty DS")
      emptyDs
    }
  }

  def getDataSet(id: Int): DataSet = {
    val ds = dataSetDS.filter(r => r.id == id)

    // id should be the PK so this shoiuld never happen
    if (ds.count() > 1)
      logging.log.warn("number of dataset rows returned is greater than 1 and is " + dataSetDS.filter(r => r.id == id).count().toString)

    if (ds.count >= 1)
      ds.take(1)(0) // assumes one row back... need to out validation tests for dups
    else { // if you don't find a DS then return a default DS with id = -1...
      logging.log.warn("no dataset found returning the empty DS")
      emptyDs
    }
  }

  def getDataSet(objectName: String, DataPersistenceId: Int): DataSet = {
    val ds = dataSetDS.filter(r => r.object_name == objectName && r.data_persistence_src_id == DataPersistenceId)

    if (ds.count() > 1)
      logging.log.warn("number of dataset rows returned is greater than 1 and is " + dataSetDS.filter(r => r.object_name == objectName).count().toString)

    if (ds.count >= 1)
      ds.take(1)(0) // assumes one row back... need to out validation tests for dups
    else { // if you don't find a DS then return a default DS with id = -1...
      logging.log.warn("no dataset found returning the empty DS")
      emptyDs
    }
  }

  def addDataSet(ds: DataSet): Unit = {
    val nint = null.asInstanceOf[Int]
    // TODO. inserting a new dataset if id is null or updating existing if id is not null
    ds.id match {
      case `nint` => // insert
      case _      => // update
    }
  }

  def extendedFromDataSet(ds: DataSet): ExtendedDataSet =
    ExtendedDataSet(
        ds.id,
        ds.format,
        ds.storage_type,
        ds.mode,
        ds.container,
        ds.object_type,
        ds.object_name,
        ds.query,
        ds.partition_key,
        ds.number_partitions,
        //   ds.priority_level,
        //   ds.dataset_state_id,
        getSchema(ds.schema_id),
        getDataPersistence(ds.data_persistence_src_id),
        getDataPersistence(ds.data_persistence_dest_id)
    )

  def getExtendedDataSet(id: Int): ExtendedDataSet = { // id is the DataSet here... NH: 8/3/2019. ExtendedDataSet will replace DataSet as we better encapsulate once

    // id should be the PK so this should never happen
    if (dataSetDS.filter(r => r.id == id).count() > 1)
      logging.log.warn("number of dataset rows returned is greater than 1 and is " + dataSetDS.filter(r => r.id == id).count().toString)

    extendedFromDataSet(dataSetDS.filter(r => r.id == id).take(1)(0))
  }

  /**
   *  DataPersistence
   *
   */
  def getDataPersistence(idLookup: Integer): com.stitchr.core.common.Encoders.DataPersistence = {
    // get the row for the data source properties as a DataPersistenceNode
    val ds = dataPersistenceDF
      .filter("id = '" + idLookup + "'")
      .select("id", "name", "persistence_type", "storage_type", "driver", "host", "port", "db", "user", "pwd", "fetchsize")
      .as(dataPersistenceEncoder)

    if (ds.count >= 1)
      ds.collectAsList()
        .get(0) // assumes one row back... need to out validation tests for dups
    else // if you don't find a DS then return a default DS with id = -1...
      emptyDp

  }

  def getDataPersistence(name: String): com.stitchr.core.common.Encoders.DataPersistence = {
    // get the row for the data source properties as a DataPersistenceNode
    val ds = dataPersistenceDF
      .filter("name = '" + name + "'")
      .select("id", "name", "persistence_type", "storage_type", "driver", "host", "port", "db", "user", "pwd", "fetchsize")
      .as(dataPersistenceEncoder)

    if (ds.count >= 1)
      ds.collectAsList()
        .get(0) // assumes one row back... need to out validation tests for dups
    else // if you don't find a DS then return a default DS with id = -1...
      emptyDp
  }

  def addDataPersistence(dp: DataPersistence): Unit =
    // TODO. inserting a new data persistence object if id is -1 (instead of null)  or updating existing if id is not null
    dp.id match {
      case -1 => // insert
      case _  => // update
    }

  /**
   * DataSet Schema space
   */
  /**
   *
   * @param schemaId
   * @return
   */
  def getSchemaDS(schemaId: Integer): Dataset[SchemaColumn] =
    schemasDF
      .filter(s"id = '$schemaId' ")
      .orderBy("column_position")
      .select("id", "column_name", "column_position", "column_type", "column_precision", "string_length", "is_nullable")
      .as[SchemaColumn]

  /**
   * schema_columns.csv looks
   * [id,column_name,column_position,column_type,column_precision,string_length,is_nullable]
   */
  def getSchema(schemaId: Integer): StructType =
    getSchema(getSchemaDS(schemaId: Integer))

  def getSchema(schemaDataSet: Dataset[SchemaColumn]): StructType = {

    if (schemaDataSet.count == 0) {
      return null.asInstanceOf[StructType] // not something encouraged in Scala... maybe change to Option/Some
    } // return a null schema value if the query returns nothing  else proceed

    val schema: StructType = StructType(
        schemaDataSet
          .collect()
          .map(
              { s =>
                StructField(
                    s.column_name,
                    toSqlType(s.column_type, s.column_precision, s.string_length).dataType,
                    s.is_nullable
                )
              }
          )
    )
    schema
  }

  def getSchemaArray(schemaId: Int): Array[SchemaColumn] =
    getSchemaDS(schemaId: Integer).collect()

  /**
  most of the externally useful APIs are here
   */
  /**
   * json DataSet object
   *
   * @param objectRef is the dataSet object ref (alternate unique key)... consists of the object_name and data presistence  reference
   * @return Json string representation of a DataSet
   */
  def getJsonDataSet(objectRef: String): String =
    toJson[DataSet](getDataSet(objectRef))

  /**
   *
   * @param id DataSet id (Primary key)
   * @return Json string representation of a DataSet
   */
  def getJsonDataSet(id: Int): String =
    toJson[DataSet](getDataSet(id))

  /**
   *
   * @param jDataSet
   */
  def putJsonDataset(jDataSet: String): Unit = {
    import com.stitchr.core.api.DataSetApi.Implicits
    val ds = fromJson[DataSet](jDataSet)
    ds.upsertDataset
    logging.log.info(s"registered DataSet $ds")
  }

  /**
   * json data persistence object
   *
   * @param id
   * @return
   */
  def getJsonDataPersistence(id: Int): String =
    toJson[DataPersistence](getDataPersistence(id))

  def getJsonDataPersistence(name: String): String =
    toJson[DataPersistence](getDataPersistence(name))

  def putJsonDataPersistence(jDataPersistence: String): Unit = {
    import com.stitchr.core.api.DataPersistenceApi.Implicits
    val dp = fromJson[DataPersistence](jDataPersistence)
    dp.registerDataPersistence
    logging.log.info(s"registered DataPersistence $dp")
  }

  /**
   *
   * @param id
   * @return
   */
  def getJsonSchema(id: Int): String =
    toJson[Array[SchemaColumn]](getSchemaArray(id))

  /**
   *
   * @param jSchema
   */
  def putJsonSchema(jSchema: String): Unit = {
    val ds = fromJson[Array[SchemaColumn]](jSchema).toList.toDF.as[SchemaColumn]
    import com.stitchr.core.api.DataSetSchemaApi.Implicits
    ds.registerDataSetSchema()
    logging.log.info(s"registered schema $ds")
  }

  /**
   *
   *
   * @return Extended Dataset where FKs are optional.
   */
  def getJsonExtendedtDataset(id: Int): String =
    toJson[ExtendedDataSet](extendedFromDataSet(getDataSet(id)))

  /**
   *
   * @param jDataSet is a json rep with id as optiponal that is passed to the function.
   * @return Dataset where FKs are optional.
   */
  def putJsonExtendedtDatasetFromDataset(jDataSet: String): ExtendedDataSet =
    extendedFromDataSet(fromJson[DataSet](jDataSet))

  /**
   *
   * @param jExtendedDataSet is a json rep with id as optiponal that is passed to the function.
   * @return Dataset where FKs are optional.
   */
  def putJsonExtendedDataset(jExtendedDataSet: String): ExtendedDataSet =
    fromJson[ExtendedDataSet](jExtendedDataSet)

  /* end registry service */
}
