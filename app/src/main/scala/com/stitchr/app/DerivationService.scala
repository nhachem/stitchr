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

import com.stitchr.util.Properties.configS3
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.dataflow.Runner

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
