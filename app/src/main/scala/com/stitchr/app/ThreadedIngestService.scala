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

/*
NH 8/1/2019 EXPERIMENTAL: adding threading support ... may change to using cats library and or scala Future
THIS IS EXPERIMENTAL Use at yr own risk.
does not work as expected yet...
 */

package com.stitchr.app

import java.util.concurrent.Semaphore

import com.stitchr.core.api.DataSet.Implicits
import com.stitchr.core.registry.RegistryService.getDataset
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.{ Logging, Threaded2 }
import com.stitchr.util.EnvConfig.sem
import com.stitchr.util.Properties.configS3

object ThreadedIngestService extends App {

  val logging = new Logging
  spark.sparkContext.setLogLevel("INFO")

  val _ = configS3() // needed for AWS ... will have to extend to include GS amd  make conditional based on config or metadata
  // just list the session info
  val configMap: Map[String, String] = spark.conf.getAll
  logging.log.info(s"configMap is $configMap")

  // using a global setup for now val sem = new Semaphore(semaphores)
  spark.catalog.listTables.show(50, truncate = false)

  def load2Lake(sem: Semaphore, q: String): Unit = {
    logging.log.info(s"loading to data lake $q")
    getDataset(q).move2Lake
    logging.log.info(s"loaded to data lake $q")
  }

  def run(ql: List[String]): Unit = {
    val t: Threaded2 = ql.foldLeft(null: Threaded2)(
        (_, next) => {
          val t = new Threaded2(sem, next, load2Lake)
          t.start()
          t
        }
    )
    t.join() // have it wait for all threads to complete
  }

  val usage =
    """
    Usage: Derivation [commaDelimitedListOfObjectRefs] ]
    """

  if (args.length < 1) println(usage)
  else {
    // note that we do not limit the number of thread counts... So all queries are started but the number of semaphores blocks more than the number of semaphores to actually run.
    // we need to test how this affects the scheduling iof executors and maybe limit the number of threads started to the number of semaphores as well
    val ql = args(0).toString.split(",").toList

    println(s"list of queries is $ql")

    run(ql)

    // show changes to catalog as we iterate.. will pull out
    // this may actually be reached before the end... So we may need to wait until all semaphores aere released?!
    println(sem.availablePermits())
    spark.catalog.listTables.show(50, truncate = false)
    logging.log.info(s"number of tables in the inSessionDB is ${spark.catalog.listTables.count()}")
  }
}
