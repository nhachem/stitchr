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

import com.stitchr.util.SharedSession.spark

import scala.collection.mutable.HashMap
import com.hubspot.jinjava._
import com.stitchr.core.util.Convert.stripCurlyBrace

import scala.collection.JavaConversions._
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object Parse {

  def getQueryObjectMap(q: String): Map[String, String] = {
    val plan = logicalPlan(stripCurlyBrace(q))
    // add a check on mode .... if database then map else keep as is...
    // NH 12/22/20 BUG: this will fail if we override with object_key... best is to do it object by object (getting the dataset object and pulling container.object_name
    plan.collect { case r: UnresolvedRelation => (r.tableName, s"""${r.tableName.split("__")(1)}.${r.tableName.split("__")(2)}""") }.toMap
  }

  def jinjaReplace(q: String, context: Map[String, String]): String = {
    val jinjava = new Jinjava
    jinjava.render(q, context)
  }

  def logicalPlan(query: String): LogicalPlan = {
    // https://stackoverflow.com/questions/49785796/how-to-get-table-names-from-sql-query
    val plan = spark.sessionState.sqlParser.parsePlan(query)
    plan
  }

  // slow parser... should look into Stream and scan in map instead of substring...
  import org.apache.spark.sql.Row
  def getColumnsInfo(schemaConfig: Config, file: String): (Int, IndexedSeq[(Int, Int, Int)]) = {
    val nbOfColumns: Int = schemaConfig.getInt("schema." + file + ".numberOfColumns")
    val colsInfo = for (i <- 0 to (nbOfColumns - 1))
      yield
        (
            i,
            schemaConfig.getInt("schema." + file + ".col" + i.toString() + ".start"),
            schemaConfig.getInt("schema." + file + ".col" + i.toString() + ".end") + 1
        )
    (nbOfColumns, colsInfo)
  }

  def parseRow(row: String, file: String, nbOfColumns: Int, colsInfo: IndexedSeq[(Int, Int, Int)]): Row = {
    val parsedRow = (for (i <- 0 to (nbOfColumns - 1))
      yield (row.substring(colsInfo(i)._2, colsInfo(i)._3))).toArray
    Row.fromSeq(parsedRow)
  }

}
