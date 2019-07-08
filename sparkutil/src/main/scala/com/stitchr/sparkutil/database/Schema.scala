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

package com.stitchr.sparkutil.database

import com.stitchr.util.Properties._
import com.typesafe.config.Config
import org.apache.spark.sql.types._

object Schema {

  // schema stuff
  case class SchemaType(dataType: org.apache.spark.sql.types.DataType, nullable: Boolean)

  /**
   * used to build a mapping from different schema daat sources... such as postgres, vertica, spark....
   * all mapping are into spark.
   * this is not complete and will be expanded as needed
   * * NH this is a subset of all possible types... best to get the list from
   * * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-DataType.html
   * * or
   * * https://spark.apache.org/docs/latest/sql-programming-guide.html#data-types
   * * and
   * * https://github.com/apache/spark/tree/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types
   *
   * @param stype
   * @param precision
   * @param scale
   * @return
   */
  def toSqlType(stype: String, precision: Int, scale: Int): SchemaType =
    stype match {
      case "integer"                => SchemaType(IntegerType, nullable = false)
      case "string"                 => SchemaType(StringType, nullable = false)
      case "character varying"      => SchemaType(StringType, nullable = false)
      case "boolean"                => SchemaType(BooleanType, nullable = false)
      case "bytes"                  => SchemaType(BinaryType, nullable = false)
      case "double"                 => SchemaType(DoubleType, nullable = false)
      case "float"                  => SchemaType(FloatType, nullable = false)
      case "long"                   => SchemaType(LongType, nullable = false)
      case "decimal"                => SchemaType(DecimalType(precision, scale), nullable = false)
      case "numeric"                => SchemaType(DecimalType(precision, scale), nullable = false)
      case "timestamp"              => SchemaType(TimestampType, nullable = false)
      case "time without time zone" => SchemaType(TimestampType, nullable = false)
      case "date"                   => SchemaType(DateType, nullable = false)
    }

  def getSchema(schemaMap: Array[(String, String)]): StructType =
    StructType(schemaMap.map({ s =>
      StructField(s._1, toSqlType(s._2, 0, 0).dataType, toSqlType(s._2, 0, 0).nullable)
    }))

  def getSchema(schemaMap: Array[(String, String, Boolean)]): StructType =
    StructType(schemaMap.map({ s =>
      StructField(s._1, toSqlType(s._2, 0, 0).dataType, s._3)
    }))

  def readSchemaSpecs(configFile: String): Config =
    // "schema.config"
    readConfig(configFile) // this should come from a global config parameter

  def getFileSchema(file: String): StructType = {
    // just to do it quickly for now best is to pass the properties in and not recompute for every call
    val schemaConfig = readSchemaSpecs("schema.config")
    // val numberOfAttributes = schemaConfig.getInt("schema." + file + ".numberOfColumns")
    val schemaDef: Array[(String, String)] = (
        for (i <- 0 until schemaConfig.getInt("schema." + file + ".numberOfColumns"))
          yield
            (
                schemaConfig.getString("schema." + file + ".col" + i.toString() + ".name"),
                schemaConfig.getString("schema." + file + ".col" + i.toString() + ".type")
            )
    ).toArray

    // generate the schema definition based on the schemadef array
    getSchema(schemaDef)
  }

  def getColumnsInfo(schemaConfig: Config, file: String): (Int, IndexedSeq[(Int, Int, Int)]) = {
    val nbOfColumns: Int = schemaConfig.getInt("schema." + file + ".numberOfColumns")
    val colsInfo =
      for (i <- 0 until nbOfColumns)
        yield
          (
              i,
              schemaConfig.getInt(s"schema.$file.col${i.toString}.start"),
              schemaConfig.getInt(s"schema.$file.col${i.toString}.end") + 1
          )
    (nbOfColumns, colsInfo)
  }

}
