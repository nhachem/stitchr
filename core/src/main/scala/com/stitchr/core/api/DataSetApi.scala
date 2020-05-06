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

package com.stitchr.core.api

import com.stitchr.core.common.Encoders.{ DataSet, emptyDs }
import com.stitchr.core.dataflow.Runner
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.registry.RegistryService.{ getDataPersistence, getDataSet }
import com.stitchr.core.util.Convert.dataSourceNode2JdbcProp
import com.stitchr.util.SharedSession.spark
import com.stitchr.util.EnvConfig._
// { databricksHiveRegistration, baseDataFolder, dataCatalogPersistence, defaultContainer, defaultFileType, logging, overrideDefaultContainer }
import com.stitchr.core.util.Hive._
import com.stitchr.core.api.ExtendedDataframe._
import com.stitchr.core.registry.DataCatalogObject.DcDataSet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object DataSetApi {

  implicit class Implicits(dataSet: DataSet) {

    /**
     * main save to target function that takes a target object and materializes it in the target persistence.
     *  We can use an overriding target container but the normal use case is to rely on catalog-based persistence target metadata
     *
     * @param fileType used for target file format (in case of a file/object store persistence. Has a default set in the properties file
     * @return (dataSet to be registered or updated, dataSet.object_ref, DataFrame associated with the dataSet)
     */
    // NH 7/25/2019: not urgent but we may be able to make it implicit if a dataset instance object (dataset + timestamp or run_id) can be manipulated inline
    def save2Target(
        fileType: String = defaultFileType // will need to specify as well
    ): (DataSet, String, DataFrame) = {
      // add session run time column if enabled
      val dfExtended = if (dataSet.add_run_time_ref) spark.table(dataSet.object_ref).addRunTimeRef else spark.table(dataSet.object_ref)
      val destPersistence = getDataPersistence(dataSet.data_persistence_dest_id)

      val (dfResult, newDS) = destPersistence.persistence_type match {
        case "file" =>
          val fileUrl =
            if (overrideDefaultContainer)
              // note: examp,e url is of the form {gsORs3ORfileORhdfs}://bucket/folder/file.filetype
              s"${destPersistence.storage_type}${destPersistence.host}/${destPersistence.db}/${dataSet.object_ref}.$fileType"
            else s"$defaultContainer/${dataSet.object_ref}.$fileType"

          logging.log.info(s"fileUrl is $fileUrl for object ref ${dataSet.object_ref}")
          dfExtended.write.format(fileType).mode(dataSet.write_mode).save(fileUrl)
          // NH for a first iteration we add the databricks hive registration to be global and if the container is null we use default
          if (databricksHiveRegistration) {
            // this copies the data dfExtended.write.format(fileType).saveAsTable(s"${dataSet.container}.${dataSet.object_name}")
            // instead register with a create to fill in (and force parquet format only for now
            val (dropDDL, createDDL) =
              generateDDL(dfExtended, if (dataSet.container != null) dataSet.container else "default", dataSet.object_name, fileUrl, fileType)
            if (appLogLevel == "INFO") {
              println(dropDDL)
              println(createDDL)
            }
            spark.sql(dropDDL)
            spark.sql(createDDL)
          }
          logging.log.info(s"reference object count is ${spark.table(dataSet.object_ref).count}")
          (
              spark.read.format(fileType).load(fileUrl),
              dataSet.copy(
                  id = -1,
                  object_name = dataSet.object_ref,
                  object_ref = s"${dataSet.object_ref}_${dataSet.data_persistence_dest_id}",
                  format = fileType,
                  storage_type = "file",
                  object_type = "file",
                  container = "",
                  mode = "base",
                  data_persistence_src_id = dataSet.data_persistence_dest_id,
                  data_persistence_dest_id = -1,
                  query = s"${dataSet.object_ref}.$fileType"
              )
          )
        case "jdbc" =>
          val dsn =
            if (overrideDefaultContainer) destPersistence
            else getDataPersistence(1)

          val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))
          jdbc.writeTable(dfExtended, dataSet.object_ref, dataSet.number_partitions, dataSet.write_mode)
          // TODO update need a dummy table all the time
          (
              spark.table(dataSet.object_ref),
              dataSet.copy(
                  id = -1,
                  object_name = dataSet.object_ref,
                  object_ref = s"${dataSet.object_ref}_${dataSet.data_persistence_dest_id}",
                  format = dsn.storage_type,
                  storage_type = "database",
                  object_type = "table",
                  container = "public", // may need to override in next versions
                  mode = "base",
                  data_persistence_src_id = dataSet.data_persistence_dest_id,
                  data_persistence_dest_id = -1,
                  query = s"${dataSet.object_ref}"
              )
          )
        // to modify to return the DF by doing a read?
        case _ =>
          logging.log.info("unsupported ... to do")
          (spark.table("dummy"), emptyDs)
      }

      logging.log.info(s"under dev value of new DS is $newDS")

      // TODO update the dataset table with the dataset new dataset object
      //
      getDataSet(dataSet.object_name, dataSet.data_persistence_dest_id)

      (newDS, dataSet.object_ref, dfResult)
    }

    /**
     * Used to handle dataset instances to be initialized in the runtime spark database (session-based only for now)
     * @param df
     */
    def materializeInSessionDb(df: DataFrame): Unit = df.createTemporaryView(dataSet.object_ref)

    /**
     *  init is the main function that takes any dataSet and associates a lazy eval dataframe object in the inSessionDB
     * @return  (viewName object_ref of the inistanatiated object, DataFrame asociated with the object_ref in the inSessionDB)
     */
    def init: (String, DataFrame) = {
      val partitionKey = dataSet.partition_key
      val numberOfPartitions = dataSet.number_partitions

      val schema = getSchema // need to trap that there is a real id > 0 ?!
      val dataPersistence = getDataPersistence(dataSet.data_persistence_src_id)
      // NH: 7/25/2019. need to add a check if the table is already instantiated in the session. If yes we will skip and log a warning
      val (datasetDF, viewName) =
        dataSet.mode match {
          case "base" =>
            dataSet.storage_type match {
              case "file" =>
                // need to make it data persistence specific
                val objectURL =
                  if (overrideDefaultContainer)
                    s"${dataPersistence.storage_type}${dataPersistence.host}/${dataPersistence.db}/${dataSet.container}/${dataSet.query}"
                  else baseDataFolder + dataSet.query
                dataSet.format match {
                  // file formats  have more options. This is a default for now only tested are csv and pipedelimited
                  case "parquet" | "delta" | "avro" | "orc" => (spark.read.format(dataSet.format).load(objectURL), dataSet.object_ref)
                  case _                                    =>
                    // csv files have more option. This is a default ... we need tab vs csv to figure out delimiter
                    val fieldDelimiter = dataSet.format match {
                      case "pipeDelimited" => '|'.toString
                      case "tab"           => '\t'.toString
                      case "csv"           => ","
                      case _               => "," // default
                    }

                    (
                        spark.read
                          .schema(schema)
                          .format("csv")
                          .option("header", "false")
                          .option("inferSchema", "false")
                          .option("delimiter", fieldDelimiter)
                          .load(objectURL),
                        // needs to be abstracted as base objects occur as object)name in queries ... s"${dataset.format}_${dataset.id}_$objectName"
                        dataSet.object_ref
                    )
                }
              case "database" =>
                // NH 7/8/2019: assume jdbc and this should be generic to any jdbc source
                // val q = s"""select * from ${dataset.object_name}""" // need to fix and use full public.name... (using dataset.container)
                // contraint on jdbc pushdown: mode = base , and query is wrapped with () unless it is  a db object
                // issue is that we can't decide on a partition key here and may be bound by performance... Need to investigate
                val q = s"""select * from ${dataSet.query} as t"""
                val dsn = getDataPersistence(dataSet.data_persistence_src_id)
                logging.log.info(s"data source persistence info is $dsn")
                val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))

                (jdbc.readDF(q, partitionKey, numberOfPartitions), dataSet.object_ref)
            }
          case "derived" =>
            Runner.run(dataSet.object_ref)
            // NH testing (spark.table(dataset.object_ref), dataset.object_name)
            (spark.table(dataSet.object_ref), dataSet.object_ref)
        }
      // datasetDF.createTemporaryView(viewName)
      materializeInSessionDb(datasetDF)
      (viewName, datasetDF)
    }

    /**
     * Takes a dataSet object associated with a JDBC source and instantiates it in the inSessionDB as a dataFrame
     * will be DEPRECATED and merged with DataSet.init
     * @return (viewName, datasetDF)
     */
    // need to merge and deprecate
    def initializeJdbcObject: (String, DataFrame) = {

      //not needed val schema = getSchema(dataset.schema_id)
      val (datasetDF, viewName) = {
        val q = s"""select * from ${dataSet.container}.${dataSet.object_name}"""
        val dsn = getDataPersistence(dataSet.data_persistence_src_id)
        val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))

        // NH: 6/27/19. note that we need to close the connection after each initialization?! unless we establish a more global sparkjdbc connection pool
        // we may need to change the use of index which is the dataSourceRef
        (jdbc.readDF(q, dataSet.partition_key, dataSet.number_partitions), dataSet.object_ref) // s"${dsn.storage_type}_${dsn.id}_${dataset.object_name}")
      }

      // datasetDF.createTemporaryView(viewName)
      materializeInSessionDb(datasetDF) // need to verify it does pick the right reference... mostly we need to extend the api to use an extended dynamic DataSet object
      (viewName, datasetDF)
    }

    /**
     * looks up DataSet attributes from the DC
     * @return DataSet.id or -1 if object is not found
     */
    def lookupDataSet: Int =
      getDataSet(dataSet.object_ref).id

    /**
     * updates the DC dataset table with the dataSet info.
     * THIS IS NOT THREAD SAFE. So it can't be used safely with multi-threaded data movement
     *  we could make it thread safe by serializing its access (actually access to the unpersist call in the DataCatalogObject)
     */
    // [WARNING] DataSet.scala:209: side-effecting nullary methods are discouraged: suggest defining as `def upsertDataset()` instead
    def upsertDataset(): Unit =
      /* lookup and if the dataset is already registered then skip!, else insert
       */
      dataCatalogPersistence match {
        case "dc" =>
          /* general scenario
          1. get jdbc connection
          2. pass the table and the dataset record
          3. upsert into the target table and assume a serial sequence... we do independent insert and update as the sql is not uniform with different databases
          4. get back the dataset object (probably best to return it as part of the call
          cureent scenario only supports new inserts
           */
          logging.log.info(s"upserting $dataSet")
          DcDataSet(dataSet).upsert
        case "file" => logging.log.warn(s"unsupported inserts/updates for dataCatalogPersistence value $dataCatalogPersistence")
        case _      => logging.log.warn(s"unsupported inserts/updates for dataCatalogPersistence value $dataCatalogPersistence")
      }

    /** applies dataSet.init.materialize
     * @return no type returned. This is a function with side effect of instantiating the dataSet and moving it to target persistence layer
     *         So there is a DB state change associated with applying this function
     */
    def move2Target: (String, DataFrame) = {
      init
      materialize
    }

    /**
     * Takes a dataSet from the inSessionDB and saves it  to the destination persistence layer and registers/updates the DC
     * @return (viewName as object_ref, dataset dataframe associated with the saved object)
     */
    def materialize: (String, DataFrame) = {
      val (newDS, viewName, datasetDF) = save2Target()

      materializeInSessionDb(datasetDF)
      if (dataCatalogUpdate) newDS.upsertDataset // updating the data catalog is controlled as we can't write through views

      logging.log.info(s"record count for $viewName is ${datasetDF.count()} ")
      (s"$viewName", datasetDF)
    }

    // API getters/setters
    // schema back may be empty... (if schema id = -1
    /**
     * used to call the registry api to get the schema from the DC associated with a dataSet
     * @return StrucType of the schema descrobing the dataSet. null if schema is not registered in the DC
     */
    def getSchema: StructType = com.stitchr.core.registry.RegistryService.getSchema(dataSet.schema_id)

  }

}
