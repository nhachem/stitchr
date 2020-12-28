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
 * script which can be used to populate the data of a postgres tpcds database from the files.
 * It goes through building the DAG so slow as the load can be performed with a simple copy.
 * But handy as a use case of Stitchr
 */

import com.stitchr.core.dataflow.Runner
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.registry.RegistryService.{getDataPersistence, initializeDataCatalogViews}
import com.stitchr.core.util.Convert.dataSourceNode2JdbcProp
import com.stitchr.util.EnvConfig.{appLogLevel, logging}

object loadTpcds {
  initializeDataCatalogViews()
  import com.stitchr.app.DerivationService
  val destPersistence = getDataPersistence(1)
  val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(destPersistence))

  val ds = new DerivationService()
  val tpcdsTablesList = spark.table("dc_datasets").filter("storage_type ='file' and mode = 'base'").select("object_ref").collect.map{r => r(0).toString}.toList
  // val tpcdsTablesList = spark.table("dc_datasets").filter("storage_type ='file' and mode = 'base'").select("object_ref").filter("object_name in ('date_dim')"). collect.map{r => r(0).toString}.toList

  // initialize in session
  //  ds.deriveQueryList(tpcdsTablesList)

  tpcdsTablesList.foldLeft()(
    (_, next) => {
      if (appLogLevel == "INFO") logging.log.info(s"computing the derived query $next") // for storage_type $st")
      Runner.run(next)
      jdbc.writeTable(spark.table(next), next.split("__")(2), 1, "overwrite")
    })


}
