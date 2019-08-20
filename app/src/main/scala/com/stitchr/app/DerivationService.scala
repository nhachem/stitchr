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

import com.stitchr.core.dataflow.ComputeService.runQueries
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.dataflow.Runner
import com.stitchr.sparkutil.database.CatalogUtil._
import com.stitchr.util.EnvConfig.logging

class DerivationService {

  spark.sparkContext.setLogLevel("INFO")

  /**
   * expected parameters are
   * List[String] as a list of query identifiers
   * and
   * target type: file or database
   */
  /**
   * edit the parameters below to go against a target dbms or files.
   */
  def deriveQueryList(ql: List[String]): Unit = {

    // instantiate the derived views
    ql.foldLeft()(
        (_, next) => {
          logging.log.info(s"computing the derived query $next") // for storage_type $st")
          Runner.run(next)
        }
    )
    // show changes to catalog as a result if appLogLevl is info
    infoListTables()
    println(s"catalog table count is $infoListTablesCount")
  }
}

// note the current version just initializes the runtime db so at the end of the run everything goes away (we do not materialise....
// will modify to have it part of ingest
object Derivation extends App {

  def run(ql: List[String], st: String = "file") = {

    val ds = new DerivationService
    logging.log.info(s"starting the derivation of  $ql")
    ds.deriveQueryList(ql)
    runQueries(ql, st)

    // NH: clear all catalog cache for reruns with updated data catalog files move to stitchrutil.Catalog
    spark.sql("clear cache").show()

    infoListTables()

    logging.log.info(s"done with the derivation of  $ql")

  }

  val usage =
    """
    Usage: Derivation [commaDelimitedQueryList] [storageType]
    """
  // expect 2 arguments. first is a list of object references and the second is a storage type...
  // we may better doing it as a list of list and decipher... but for now it is fine
  if (args.length < 2) println(usage)
  else {

    val ql = args(0).toString.split(",").toList
    val storageType = args(1)
    println(s"list of queries is $ql")
    run(ql, storageType)
  }

}
