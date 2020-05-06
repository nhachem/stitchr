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
import org.apache.spark.sql.functions.{ col, expr }

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

      // val l = s"'${pivotColumns.mkString("','")}'"
      // replace "." with "++" in column nameees
      val l = pivotColumns
        .foldLeft("")(
            (head, next) => { s"${head}'${next}' ${next.replace(".", "__")}," }
        )
        .stripSuffix(",")
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

    // just get the sql to use in hive ... replacing . with $ in output column names
    // note that _tmp will need to be replaced with source table/view if we use the query to establish a hive view/table
    def genPivotSQL(pivotedColumnsList: List[String] = null, fn: String = "max"): String = {
      val pivotColumns =
        if (pivotedColumnsList == null) dataFrame.select("key").distinct.map(r => s"${r(0)}").collect.toList
        else pivotedColumnsList

      // val l = s"'${pivotColumns.mkString("','")}'"
      val l = pivotColumns
        .foldLeft("")(
            (head, next) => { s"${head}'${next}' ${next.replace(".", "__")}," }
        )
        .stripSuffix(",")
      dataFrame.createOrReplaceTempView("_tmp")

      val q = s"""SELECT * FROM
                 | (
                 | SELECT *
                 | FROM _tmp
                 | )
                 | PIVOT (
                 | max(value)
                 | FOR key in ( $l )
                 | ) """.stripMargin
      q
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

    // from https://www.24tutorials.com/spark/flatten-json-spark-dataframe/
    // this is not tail recursive but hopefully will not matter
    import org.apache.spark.sql.types.{ StructType, ArrayType }
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.Column
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.functions.explode_outer
    def flatten: DataFrame = {

      val fields = dataFrame.schema.fields
      val fieldNames = fields.map(x => x.name)
      val length = fields.length

      for (i <- 0 to fields.length - 1) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          case arrayType: ArrayType =>
            val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
            // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
            val explodedDf = dataFrame.selectExpr(fieldNamesAndExplode: _*)
            return explodedDf.flatten
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "__"))))
            val explodedf = dataFrame.select(renamedcols: _*)
            return explodedf.flatten
          case _ =>
        }
      }
      dataFrame
    }

    def flattenNoExplode: DataFrame = {

      val fields = dataFrame.schema.fields
      val fieldNames = fields.map(x => x.name)
      val length = fields.length

      for (i <- 0 to fields.length - 1) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          // only handle strycttype and skip arrays
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
            val explodedf = dataFrame.select(renamedcols: _*)
            return explodedf.flattenNoExplode
          case _ =>
        }
      }
      dataFrame
    }

    /**
     * Takes a dataframe, assumes first column is PK and unpivots all other columns using the stack function
     * @return
     */
    def unPivot() = {
      val schemaStruct = dataFrame.schema
      // schemaStruct.toDDL // show the string fields as a DDL representation
      // schemaStruct.fieldNames.mkString("'", "','", "'")
      val schemaArrayNames = schemaStruct.fieldNames
      // assume the pk is the 1st column (we may need to adjust and look for the one that has PK!... but this should preferable not be used
      // if we have multiple columns as pk maybe we need a parameter that selects a few columns... we will look at this issue when we hit it
      val stackFieldsArray = schemaArrayNames.drop(1)
      val stackFields = s"stack(${stackFieldsArray.size.toString}" + schemaArrayNames
        .drop(1)
        .toList
        .foldLeft("")((head, next) => { s"$head, '$next', `$next` " }) + ")"

      dataFrame.select(col(s"${schemaArrayNames.head}"), expr(s"$stackFields as (key_column,value)"))
    }

  }

}
