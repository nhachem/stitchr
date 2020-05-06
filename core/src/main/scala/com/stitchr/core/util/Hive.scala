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

package com.stitchr.core.util

import java.sql.{ ResultSet, ResultSetMetaData, SQLException }

import com.stitchr.core.common.Encoders.{ Column, DataPersistence }
import com.stitchr.core.dbapi.PostgresDialect
import com.stitchr.util.SharedSession.spark
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row }

// import scala.util.parsing.json.JSONObject

// import com.stitchr.core.common.Encoders.JdbcProps
import com.stitchr.util.database.JdbcProps
import com.typesafe.config.Config

import scala.collection.mutable.ArrayBuffer

object Hive {
  // NH: 5/4/2020 under development
  //assumes the dataframe is written to storage somewhere else
  def generateDDL(df: DataFrame, schemaName: String, tableName: String, fileURL: String, storageType: String = "delta"): (String, String) = {
    //get the schema split as string with comma-separated field-datatype pairs
    val schema: StructType = df.schema
    // can do that with mkStrings + prefix and suffix...
    // also the real solution is to quote the column names to handle the .?!
    val columns =
      df.schema.fields.toList.foldLeft("")((head, next) => s"$head${(next.name).replace(".", "_")} ${next.dataType.typeName}\n,").replaceAll(",$", "")
    //drop the table if already created
    val dropDDL = s"drop table if exists ${schemaName}.${tableName}"
    //create the table using the dataframe schema
    // val createDDL = s"""create table if not exists $schemaName.$tableName ($columns)
    //val createDDL = s"""create table $schemaName.$tableName ($columns)
    //                   | USING $storageType location '$fileURL'""".stripMargin
    val createDDL = s"""create table $schemaName.$tableName
                       | USING $storageType location '$fileURL'""".stripMargin
    (dropDDL, createDDL)
  }
  def listColumns(schemaName: String, tableName: String): List[String] =
    //spark.sql(s"describe ${schemaName}.${tableName}").select("columnName").collect.map(r => r(0).toString()).toList
    // or
    spark.table(s"${schemaName}.${tableName}").select("columnName").collect.map(r => r(0).toString()).toList

  def listColumnsDF(schemaName: String, tableName: String): DataFrame =
    //spark.sql(s"describe ${schemaName}.${tableName}").select("columnName").collect.map(r => r(0).toString()).toList
    // or
    spark.table(s"${schemaName}.${tableName}").select("columnName")

  def listTables(schemaName: String): List[String] = {
    spark.sql(s"use ${schemaName}")
    spark.sql(s"show tables").select("tableName").collect.map(r => r(0).toString()).toList
  }

  def listTablesDF(schemaName: String): DataFrame = {
    spark.sql(s"use ${schemaName}")
    spark.sql(s"show tables").select("tableName")
  }
}
