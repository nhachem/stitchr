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

import com.stitchr.util.EnvConfig.props
import com.stitchr.core.dataflow.ComputeService.runQueries
import com.stitchr.util.Properties.configS3
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.dataflow.Runner
import com.stitchr.util.Logging

class DerivationService {
  val logging = new Logging

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
  def deriveQueryList(ql: List[String]): Unit = { // , st: String = "file"): Unit = {

    val _ = configS3() // needed for AWS ... will have to extend to include GS amd  make conditional based on config or metadata

// instantiate the derived views
    ql.foldLeft()(
        (_, next) => {
          println(s"computing the derived query $next") // for storage_type $st")
          Runner.run(next) // , st)
          // show changes to catalog as we iterate.. will pull out
          spark.catalog.listTables.show(50, false)
          spark.catalog.listTables.count()
        }
    )

  }
}

// limited to 22... but will work for most of our needs
// case class configParam(globalPartitions: Int = 8, globalFileName: String = null, outDir: String = null)
// note the current version just initializes the runtime db so at the end of the run everything goes away (we do not materialise....
// will modify to have it part of ingest
object Derivation extends App {

  def run(ql: List[String], st: String = "file") = {
    val logging = new Logging
    spark.sparkContext.setLogLevel("INFO")
    val ds = new DerivationService
    logging.log.info(s"starting the derivation of  $ql")
    ds.deriveQueryList(ql)
    runQueries(ql, st)

    // clear all catalog cache for reruns with updated data catalog files
    spark.sql("clear cache").show()

    spark.catalog.listTables.show(50, false)

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
