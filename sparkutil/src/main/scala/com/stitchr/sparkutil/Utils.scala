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

package com.stitchr.sparkutil

import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.EnvConfig.{ defaultTmpContainer, defaultFileType }

object Utils {

  import org.apache.spark.sql.{ DataFrame }

  import scala.util.Random

  // provides a way to self checkpoint a DataFrame and return a new DF from the saved file
  // object is placed in the tmp persistence storage space (to be cleaned externally)
  def selfCheckpoint(
      df: DataFrame,
      fileName: String = "tmp" + Random.nextInt(1000).toString,
      container: String = defaultTmpContainer,
      fileType: String = defaultFileType
  ): (String, DataFrame) = {
    val fileUrl = s"$container/$fileName.$fileType"
    // log , s"writing ${fileType} temp checkpoint ${fileOut}")
    df.write.format(fileType).mode("overwrite").save(fileUrl)
    (fileName, spark.read.format(fileType).load(fileUrl + "." + fileType))
  }

}
