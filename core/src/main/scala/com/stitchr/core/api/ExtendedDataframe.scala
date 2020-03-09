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

import com.stitchr.util.SharedSession.spark
import com.stitchr.util.EnvConfig._
import org.apache.spark.sql.DataFrame

object ExtendedDataframe {
  // is this a hack?! will do for now to add a session_run_time
  // we could impliment as a crossjoin but this may be better.
  // import org.apache.spark.sql.functions._
  // fake it and return a value... can use any column
  // def runTimeValue( id: Int): Long = _ => sessionRunTime
  // val sqlValue = udf(runTimeValue(0))
  // using a sequence
  import spark.implicits._
  // maybe need to set spark.sql.crossJoin.enabled=true
  val sessionRunTimeAttribute = "session_run_time"
  val df = Seq(sessionRunTime).toDF(sessionRunTimeAttribute)

  implicit class DataFrameImplicits(dataFrame: DataFrame) {

    def addRunTimeRef: DataFrame =
      // trivial match
      dataFrame.schema.fieldNames contains sessionRunTimeAttribute match {
        case true  => dataFrame
        case false => df.crossJoin(dataFrame)
      }

    def createTemporaryView(viewName: String): Unit =
      if (spark.catalog.tableExists(viewName)) logging.log.warn(s"view name $viewName exists, so skipping")
      else dataFrame.createTempView(viewName)

    // assume the pivot columns are key, value
    // provide the list as a parameter as sometimes we do not need a full pivot. null means all and we use the schema to generate the pivoted columns
    def pivot(pivotedColumnsList: List[String] = null, fn: String = "max"): DataFrame = {
      val pivotColumns =
        if (pivotedColumnsList == null) dataFrame.select("key").distinct.map(r => s"${r(0)}").collect.toList
        else pivotedColumnsList

      val l = s"'${pivotColumns.mkString("','")}'"
      dataFrame.createOrReplaceTempView("_tmp")

      val q = s"""SELECT * FROM
                  | (
                  | SELECT *
                  | FROM _tmp
                  | )
                  | PIVOT (
                  | $fn(value)
                  | FOR key in ( $l )
                  | ) """.stripMargin
      spark.sql(q)
    }

    /**
     * transforms a dataframe with a string json column columnName into a struc
     * Note: does not handle cleanly columns that are null or have an array of null.
     * @param columnName Json column to transform
     * @return transformed dataframe
     */
    def cast2Json(columnName: String): DataFrame = {
      import org.apache.spark.sql.functions._
      val schema = spark.sqlContext.read.json(dataFrame.select(columnName).as[String]).schema
      dataFrame
        .withColumn(s"${columnName}_jsonString", from_json(col(columnName), schema))
        .drop(columnName)
        .withColumnRenamed(s"${columnName}_jsonString", columnName)
    }
  }

}
