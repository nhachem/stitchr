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

/**
 * Used to demo use cases
 * 1. Source and Target are a database
 * 2. Source and Target are Spark files and Spark warehouse
 *
 * The data is based on tpcds.
 * data files are stripped from the last | to make them directly readable and are stored under demo/data/tpcds
 * generate the data using tpcds tools and place in that directory before the demo.
 * You can find a copy ready to use under nhachem/stitchr-demo
 */
package com.stitchr.app

import com.stitchr.core.api.DataSetApi.Implicits
import com.stitchr.core.registry.RegistryService.{ getDataSet, getObjectRef, getQueryReferenceList }
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.EnvConfig.logging
import com.stitchr.util.Util.time
import com.stitchr.sparkutil.database.CatalogUtil._

object DataMoveService {

  /**
   * TO BE DEPRECATED. Assumes that a query is unique within either file or database storage persistence types; which is not necessarily true
   * @param ql expected parameters are
   * List[String, Int] as a list of query identifiers as dataset.object_name, dataPersistence.data_persistence_id.
   * It assumes that those were already derived and abstracted in the spark database. in other words the local db has been instantiated
   * and target type: file or database
   *
   */
  def instantiateQueryList(ql: List[(String, Int)]): Unit = {

    // assumed that the inSessionDb is fully populated so no need to derive
    // val ds = new DerivationService
    // ds.deriveQueryList(ql)

    // Then run the actual materialization of the target objects

    ql.foldLeft()(
        (_, next) => {
          logging.log.info(s"instantiating the target for $next")
          val (viewName, dfm) = getDataSet(getObjectRef(next._1)).materialize
          logging.log.info(s"viewname is $viewName")
          spark.sparkContext.setLogLevel("INFO")
          // NH: this is not needed longer term
          time(dfm.count, "counting the rows in the materialized object")
          // dfm.printSchema()

        }
    )
  }

  /**
   *  this is focusing on one type of persistence
   * @param ql List[object_name]
   * @param dataStorage file or database
   */
  def instantiateQueryList(ql: List[String], dataStorage: String): Unit = { // , st: String = "file"): Unit = {

    // assumed that the inSessionDb is fully populated so no need to derive
    // val ds = new DerivationService
    // ds.deriveQueryList(ql)

    // Then run the actual materialization of the target objects
    ql.foldLeft()(
        (_, next) => {
          logging.log.info(s"instantiating the target for $next")
          println(getObjectRef(next, dataStorage))
          val (viewName, dfm) = getDataSet(getObjectRef(next, dataStorage)).materialize
          // for debugging fr now
          println(s"viewname is $viewName")
          time(dfm.count, "counting the rows in the materialized object")
          dfm.printSchema()

        }
    )
  }

  /** moveDataSetList takes a list of object_refs and moves them to the target persistence zone with DataSet.move2Target
   *
   * @param ql is a list of object_ref computed as <object_name>_<data_persistence_src_id> from the dataset DC table
   */
  def moveDataSetList(ql: List[String]): Unit = {

    // instantiate the derived views
    ql.foldLeft()(
        (_, next) => {
          logging.log.info(s"loading to data target $next") // for storage_type $st")
          getDataSet(next).move2Target

        }
    )
    // show changes to catalog as we iterate.. will pull out
    infoListTables()
    logging.log.info(s"number of table in the inSessionDB is ${infoListTablesCount}")
  }

}

object MoveDataSetGroup extends App {

  // just list the session info
  val configMap: Map[String, String] = spark.conf.getAll
  logging.log.info(s"configMap is $configMap")

  val usage =
    """
    Usage: MoveDatSetGroup [group_name] ]
    """
  // expect 2 arguments. first is a list of object references and the second is a storage type...
  // we may better doing it as a list of list and decipher... but for now it is fine
  if (args.length != 1) println(usage)
  else {

    val groupName = args(0).toString
    val ql = getQueryReferenceList(groupName)

    logging.log.info(s"list of queries is $ql")
    DataMoveService.moveDataSetList(ql)
  }

}

/*
may replace the DataIngestService (or be merged with it)
 */
object MoveDataSetList extends App {

  // just list the session info
  val configMap: Map[String, String] = spark.conf.getAll
  logging.log.info(s"configMap is $configMap")

  val usage =
    """
    Usage: IngestDataSet [commaDelimitedObjectRef] ]
    """
  // expect 2 arguments. first is a list of object references and the second is a storage type...
  // we may better doing it as a list of list and decipher... but for now it is fine
  if (args.length < 1) println(usage)
  else {

    val ql = args(0).toString.split(",").toList
    logging.log.info(s"list of queries is $ql")
    DataMoveService.moveDataSetList(ql)
  }

}
