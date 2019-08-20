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

import com.stitchr.core.common.Encoders.DataPersistence
import com.stitchr.core.registry.DataCatalogObject._
import com.stitchr.core.registry.RegistryService.getDataPersistence
import com.stitchr.core.common.Encoders.SchemaColumn
import com.stitchr.util.EnvConfig._
import org.apache.spark.sql.Dataset
object DataSetSchemaApi {

  implicit class Implicits(dataSetSchema: Dataset[SchemaColumn]) {

    /**
     * THIS IS NOT THREAD SAFE. So it can't be used safely with multi-threaded data movement
     *  we could make it thread safe by serializing its access (actually access to the unpersist call)
     */
    //  [WARNING] DataPersistence.scala:24: side-effecting nullary methods are discouraged: suggest defining as `def registerDataPersistence()` instead
    def registerDataSetSchema(): Unit =
      /* lookup and if the dataset is already registered then skip!, else insert
       */
      dataCatalogPersistence match {
        case "dc" =>
          // DcDataSet(ds).delete
          logging.log.info(s"upserting $dataSetSchema")
          DcDataSchema(dataSetSchema).insert // nop update supported
        case "file" => logging.log.warn(s"not supporting inserts/updates for dataCatalogPersistence value $dataCatalogPersistence")
        case _      => logging.log.warn(s"unsupported dataCatalogPersistence value $dataCatalogPersistence")
      }

  }

}
