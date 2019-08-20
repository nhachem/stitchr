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
import com.stitchr.core.common.Encoders.{ DataPersistence, DataSet, SchemaColumn }
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.util.database.JdbcImpl
import com.stitchr.core.registry.DataCatalogObject.{ insertDB, updateDB }
import com.stitchr.core.registry.RegistrySchema.{ dataPersistenceDF, dataSetDF, schemasDF }
import com.stitchr.core.registry.RegistryService.{ getDataPersistence, getDataSet }
import com.stitchr.core.util.Convert.config2JdbcProp
import com.stitchr.util.EnvConfig.{ logging, props, dataCatalogPersistence }
import org.apache.spark.sql.{ DataFrame, Dataset }
import spark.implicits._

abstract class DataCatalogObject {

  def insert[T]: Int // may change to return the id...
  def update[T]: Int //

}

object DataCatalogObject {

  def insertDB(df: DataFrame, tableName: String): Unit = {
    // assumes all entries are new so inserts without the id column
    val sparkJdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
    sparkJdbc.writeTable(df.drop(df.col("id")), tableName, 1, "append")
  }

  def updateDB(updSql: String): Int = {
    val jdbc = new JdbcImpl(config2JdbcProp(props, "dc"))
    jdbc.executeUpdate(updSql)
  }

  def deleteDB(delSql: String): Unit = {
    val jdbc = new JdbcImpl(config2JdbcProp(props, "dc"))
    jdbc.executeDDL(delSql)
  }

  def executeDDL(sql: String): Boolean = {
    val jdbc = new JdbcImpl(config2JdbcProp(props, "dc"))
    jdbc.executeDDL(sql)
  }

  def dbPersistence: Boolean =
    if (dataCatalogPersistence == "dc") true
    else false

  def sqlTemplate(tableName: String): (String, String) =
    ("to do insert", "to do update")

  case class DcDataSchema(dataSetSchema: Dataset[SchemaColumn]) extends DataCatalogObject {

    //NH: 8/15/2019 Note that we need to make the counts as Long not Int...
    override def insert[T]: Int =
      if (dbPersistence) {
        // simple DF approach
        val sparkJdbc = SparkJdbcImpl(config2JdbcProp(props, "dc"))
        sparkJdbc.writeTable(dataSetSchema.toDF, "schema_column", 1, "append")
        schemasDF.unpersist() // this works to force a refresh but is NOT THREAD SAFE as it is not transactional
        dataSetSchema.count().toInt // hack for now
      } else -1

    override def update[T]: Int = {
      //TODO
      logging.log.warn("schema update not implemented yet")
      0
    }

  }

  case class DcDataSet(dataSet: DataSet) extends DataCatalogObject {

    // id is automatically generated as a serial if not provided (which is assumed here)
    val insertString =
      """insert into dataset(
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
        | data_persistence_dest_id)
      """.stripMargin

    // val values = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val insertValues = s""" values(
   | '${dataSet.format}',
   | '${dataSet.storage_type}',
   | '${dataSet.mode}',
   | '${dataSet.container}',
   | '${dataSet.object_type}',
   | '${dataSet.object_name}',
   | '${dataSet.query}',
   | '${dataSet.partition_key}',
   | ${dataSet.number_partitions},
   | ${dataSet.schema_id},
   | ${dataSet.data_persistence_src_id},
   | ${dataSet.data_persistence_dest_id})""".stripMargin

    val updateStatement = s"""update dataset set
                           | format = '${dataSet.format}',
                           | storage_type = '${dataSet.storage_type}',
                           | mode = '${dataSet.mode}',
                           | container = '${dataSet.container}',
                           | object_type = '${dataSet.object_type}',
                           | object_name = '${dataSet.object_name}',
                           | query = '${dataSet.query}',
                           | partition_key = '${dataSet.partition_key}',
                           | number_partitions = ${dataSet.number_partitions},
                           | schema_id = ${dataSet.schema_id},
                           | data_persistence_src_id = ${dataSet.data_persistence_src_id},
                           | data_persistence_dest_id = ${dataSet.data_persistence_dest_id}
                           | where id =  ${dataSet.id}""".stripMargin

    val deleteStatement = s""" delete from dataset where id = ${dataSet.id}"""

    val insertStatement = insertString + insertValues

    override def insert[T]: Int =
      if (dbPersistence) {
        // simple DF approach
        val df = Seq(dataSet).toDF
        // need to drop object_ref as it is a virtual attribute
        logging.log.info(s"insert statement is \n $insertStatement")
        insertDB(df.drop(df.col("object_ref")), "dataset")
        dataSetDF.unpersist() // this works to force a refresh but is NOT THREAD SAFE as it is not transactional
        1 // hack for now
      } else -1

    override def update[T]: Int =
      if (dbPersistence) {
        logging.log.info(s"update statement is \n $updateStatement")
        updateDB(updateStatement)
      } else -1

    def delete: Int =
      if (dbPersistence) {
        logging.log.info(s"delete statement is \n $deleteStatement")
        updateDB(deleteStatement)
      } else -1

    def upsert[T]: Int =
      if (dbPersistence)
        if (dataSet.id == -1) {
          // NH: 8/14/2019using the construct for object_ref to make sure that we enforce properly the constraint ...
          // we will move towards setting object_ref as a derived attribute in the the dataset api
          // if (getDataSet(dataSet.object_ref).id == -1) insert[T]
          if (getDataSet(s"${dataSet.object_name}_${dataSet.data_persistence_src_id}").id == -1) insert[T]
          else
            DcDataSet(dataSet.copy(id = getDataSet(s"${dataSet.object_name}_${dataSet.data_persistence_src_id}").id)).update
        } else update[T]
      else -1
  }
  case class DcDataPersistence(dataPersistence: DataPersistence) extends DataCatalogObject {
    val insertString =
      """insert into data_persistence(
      | id,
      | name,
      | persistence_type,
      | storage_type,
      | host,
      | port,
      | db,
      | user,
      | pwd,
      | driver,
      | fetchsize)
    """.stripMargin
    // val insertValues = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
    val insertValues =
      s"""(
       | ${dataPersistence.name},
       | ${dataPersistence.persistence_type},
       | ${dataPersistence.storage_type},
       | ${dataPersistence.host},
       | ${dataPersistence.port},
       | ${dataPersistence.db},
       | ${dataPersistence.user},
       | ${dataPersistence.pwd},
       | ${dataPersistence.driver},
       | ${dataPersistence.fetchsize})""".stripMargin

    val updateString =
      """update data_persistence(
      | id,
      | name,
      | persistence_type,
      | storage_type,
      | host,
      | port,
      | db,
      | user,
      | pwd,
      | driver,
      | fetchsize)
    """.stripMargin
    val insertStatement = insertString + insertValues

    val updateStatement =
      s"""update dataset set
       | name = ${dataPersistence.name},
       | persistence_type = ${dataPersistence.persistence_type},
       | storage_type = ${dataPersistence.storage_type},
       | host = ${dataPersistence.host},
       | port = ${dataPersistence.port},
       | db = ${dataPersistence.db},
       | user = ${dataPersistence.user},
       | pwd = ${dataPersistence.pwd},
       | driver = ${dataPersistence.driver},
       | fetchsize = ${dataPersistence.fetchsize}
       | where id =  ${dataPersistence.id}""".stripMargin

    val deleteStatement = s""" delete from data_persistence where id = ${dataPersistence.id}"""

    override def insert[T]: Int =
      if (dbPersistence) {
        // simple DF approach
        insertDB(Seq(dataPersistence).toDF, "data_persistence")
        logging.log.info(s"insert statement is \n $insertStatement")
        dataPersistenceDF.unpersist() // this works to force a refresh but is NOT THREAD SAFE as it is not transactional
        1 // hack for now
      } else -1

    // updates need conventional JDBC as DF do not support that, unless I do a full overwrite
    override def update[T]: Int =
      if (dbPersistence) {
        logging.log.info(s"update statement is \n $updateStatement")
        updateDB(updateStatement)
      } else -1
    def delete: Int =
      if (dbPersistence) {
        logging.log.info(s"delete statement is \n $deleteStatement")
        updateDB(deleteStatement)
      } else -1

    def upsert[T]: Int =
      if (dbPersistence) {
        getDataPersistence(dataPersistence.name).id
        if (dataPersistence.id == -1) {
          if (getDataPersistence(dataPersistence.name).id == -1) {
            insert[T]
          } else {
            DcDataPersistence(dataPersistence.copy(id = getDataPersistence(dataPersistence.name).id)).upsert
          }
        } else update[T]
      } else -1
  }
}
