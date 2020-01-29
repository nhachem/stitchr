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

package com.stitchr.app

import com.stitchr.core.common.Encoders.{ DataSet, DataPersistence, dataSetEncoder }
import com.stitchr.core.api.DataSetApi.Implicits
import com.stitchr.core.util.Convert.dataSourceNode2JdbcProp
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.database.JdbcImpl
import com.stitchr.util.EnvConfig.logging
import spark.implicits._
import org.apache.spark.sql.Dataset

object AutoRegisterService {

  /**
   * Note: in Postgres information_schema and pg_catalog seem to be hidden for JDBC metadata. A "feature" of Postgres
   * @param dataPersistence refers to the the id of the registered data persistence source in the data catalog (data_persistence table)
   * @param schemaName target schema
   * @param objectTypes Array of objects: default to TABLE and VIEW
   * @return
   */
  def getJdbcDataSets(
      dataPersistence: DataPersistence,
      schemaName: String = "public",
      objectTypes: Array[String] = Array("TABLE", "VIEW")
  ): Dataset[DataSet] = {

    println(dataPersistence)
    val jdbc = JdbcImpl(dataSourceNode2JdbcProp(dataPersistence))

    logging.log.info(s"getting Tables metadata for dataPersistence $dataPersistence and schema $schemaName")
    val rs = jdbc.getTablesMetadata(schemaPattern = schemaName, tableNamePattern = "%", objectTypes)

    val (_, results) = jdbc.getQueryResult(rs)
    // NH: BUG bad side effect. if we apply any operation on results we effectively empty it!!
    // results.size

    val ds = results
      .map { r =>
        DataSet(
            id = -1,
            object_ref = s"${r(2)}_${dataPersistence.id}", //important: virtual column based on object_name and data_persistence_id
            format = dataPersistence.storage_type,
            storage_type = "database",
            mode = "base",
            container = r(1),
            object_type = r(3).toLowerCase,
            object_name = s"${r(2)}",
            query = s"${r(2)}",
            partition_key = "",
            number_partitions = 1,
            schema_id = -1,
            data_persistence_src_id = dataPersistence.id,
            data_persistence_dest_id = -1
        )
      }
      .toList
      .toDF()
      .as(dataSetEncoder)

    ds
  }

  /**
   *
   * @param dsDS The call to getJdbcDataSets returns a Dataset [DatSet] to be registered on the DC (dataset table)
   */
  def registerJdbcDataSets(dsDS: Dataset[DataSet]): Unit =
    dsDS
      .collect()
      .toList
      .foldLeft()((_, next) => {
        // println(next)
        next.upsertDataset()
      })

}
