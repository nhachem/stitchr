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

import com.stitchr.core.dataflow.Runner
import com.stitchr.util.database.CatalogUtil._
import com.stitchr.util.EnvConfig.{ appLogLevel, logging }

class DerivationService {

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
          if (appLogLevel == "INFO") logging.log.info(s"computing the derived query $next") // for storage_type $st")
          Runner.run(next)
        }
    )
    // show changes to catalog as a result if appLogLevel is info
    if (appLogLevel == "INFO") {
      infoListTables()
      println(s"catalog table count is $infoListTablesCount")
    }
  }
}
