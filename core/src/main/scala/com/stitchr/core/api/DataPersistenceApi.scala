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

import com.stitchr.core.common.Encoders.{ DataPersistence, emptyDp }
import com.stitchr.core.registry.DataCatalogObject.DcDataPersistence
import com.stitchr.core.registry.RegistrySchema.dataPersistenceDF
import com.stitchr.core.registry.RegistryService.{ getDataPersistence }
import com.stitchr.util.EnvConfig._

object DataPersistenceApi {

  implicit class Implicits(dataPersistence: DataPersistence) {

    def lookupDataPeristence: Int = {
      // TODO
      //  returns id = -1 if not found
      getDataPersistence(dataPersistence.name)
      -1
    }

    /**
     * THIS IS NOT THREAD SAFE. So it can't be used safely with multi-threaded data movement
     *  we could make it thread safe by serializing its access (actually access to the unpersist call)
     */
    //  [WARNING] DataPersistence.scala:24: side-effecting nullary methods are discouraged: suggest defining as `def registerDataPersistence()` instead
    def registerDataPersistence(): Unit =
      /* lookup and if the dataset is already registered then skip!, else insert
       */
      dataCatalogPersistence match {
        case "dc" =>
          /* general scenario
          1. get jdbc connection
          2. pass the table and the dataPersistence record
          3. upsert into the target table and assume a serial sequence... we do independent insert and update as the sql is not uniform with different databases
          4. get back the dataPersistence object (probably best to return it as part of the call
          current scenario only supports new inserts
           */
          // DcDataSet(ds).delete
          logging.log.info(s"upserting $dataPersistence")
          DcDataPersistence(dataPersistence).upsert
        case "file" => logging.log.warn(s"not supporting inserts/updates for dataCatalogPersistence value $dataCatalogPersistence")
        case _      => logging.log.warn(s"unsupported dataCatalogPersistence value $dataCatalogPersistence")
      }

  }

}
