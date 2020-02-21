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

import com.stitchr.core.common.Encoders.{ Column, DataPersistence, QueryNode, SchemaColumn }
import com.stitchr.util.SharedSession.spark
import com.stitchr.core.dbapi.PostgresDialect
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getSchema
import java.sql.{ ResultSet, ResultSetMetaData, SQLException }

// import scala.util.parsing.json.JSONObject

// import com.stitchr.core.common.Encoders.JdbcProps
import com.stitchr.util.database.JdbcProps
import com.typesafe.config.Config

import scala.collection.mutable.ArrayBuffer

object Convert {
  /* pull out for now  as it is not used anymore
  // case class JdbcProps(dbms: String, driver: String, host: String, port: Int, database: String, user: String = null, pwd: String = null, fetchsize: Int = 10000)
 def fromRow2JdbcProp(r: Row): JdbcProps =
   //for user/pwd we will use the sys env for now
   JdbcProps(r.get(1).toString, r.get(2).toString, r.get(3).toString, r.get(4).asInstanceOf[Int], r.get(5).toString, null, null)

  def row2JdbcProp(r: Row): JdbcProps =
    //for user/pwd we will use the sys env for now
    JdbcProps(r.get(1).toString, r.get(2).toString, r.get(3).toString, r.get(4).asInstanceOf[Int], r.get(5).toString, null, null)
   */

  /**
   * used for testing of the metadata code for now
   */
  // we use a dbIndex to specify general data source jdbc or use it for teh data catalog reference
  def config2JdbcProp(c: Config, dbIndex: String = "jdbc"): JdbcProps =
    JdbcProps(
        c.getString(s"$dbIndex.dbengine"),
        c.getString(s"$dbIndex.driver"),
        c.getString(s"$dbIndex.host"),
        c.getInt(s"$dbIndex.port"),
        c.getString(s"$dbIndex.db"),
        dbIndex,
        c.getString(s"$dbIndex.user"),
        c.getString(s"$dbIndex.pwd")
    )

  def dataSourceNode2JdbcProp(dataSource: DataPersistence): JdbcProps =
    // here we retrieve the url (jdbc connection or other) for the datasource
    JdbcProps(
        dataSource.storage_type,
        dataSource.driver,
        dataSource.host,
        dataSource.port,
        dataSource.db,
        dataSource.persistence_type, // "jdbc"
        dataSource.user,
        dataSource.pwd,
        dataSource.fetchsize
    )

  // inspired from https://stackoverflow.com/questions/1226555/case-class-to-map-in-scala
  def caseClass2Map(cc: AnyRef): Map[String, String] =
    (Map[String, String]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc).toString)
    }

  // or maybe simpler for now
  import org.json4s.{ Extraction, _ }

  def cC2Map(cc: AnyRef): Map[String, String] =
    Extraction.decompose(cc)(DefaultFormats).values.asInstanceOf[Map[String, String]]
// this does not seem right

  def getCCParams(cc: Product) = {
    val values = cc.productIterator
    cc.getClass.getDeclaredFields.map(_.getName -> values.next).toMap
  }

  /*
  This function is weak... not including for now
  import scala.util.parsing.json.JSONType
  def convertRowToJSONRow(row: Row): JSONObject = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m)
  }
   */

  /**
   * warning the following converter are applied on result sets and are memory intensive as they generate lists and use memory linearly with the resultset cardinality
   * So they are useful for small sets associated with metadata and catalog information
   */
  // not used ... yet
  @throws[SQLException]
  def resultSetMeta2List(rsmd: ResultSetMetaData): List[Column] =
    try {
      val columnCount = rsmd.getColumnCount
      val columns = new ArrayBuffer[Column]
      var i = 1
      while (i <= columnCount) {
        columns += Column(i, rsmd.getColumnName(i), rsmd.getColumnTypeName(i))
        i += 1
      }
      columns.toList
    } finally {
      // rsmd
    }

  // EXPERIMENTAL and incomplete. need to pull the hard-coded reference to the PostgresDialect
  @throws[SQLException]
  def resultSet2List(rs: ResultSet): (ArrayBuffer[Row], StructType, Int) =
    try {
      val rows = new ArrayBuffer[Row]
      val rsMetadata: ResultSetMetaData = rs.getMetaData
      val columnCount = rsMetadata.getColumnCount
      //val rsmdl = resultSetMeta2List(rsMetadata)
      println("table metadata schema")
      var j = 1
      while ({ j <= rsMetadata.getColumnCount }) {
        println("Column Name is: " + rsMetadata.getColumnName(j))
        println("Column Type is: " + rsMetadata.getColumnTypeName(j))
        // Do stuff with name
        j += 1
      }
      // schema is position|       name|att_type|
      //val a = rsmdl.map(r => (r.name, r.att_type)).toArray
      // val schema = generateStructSchema(a)

      // Prefer Spark's getSchema function
      // issue is to find the dialect or use a default as a catch all (maybe write a default one or just refer to the PostgresDialect always...
      // need to use the jdbc prefix to match to the registred dialect!! this is undocumented. this would be added ot the datasource metadata :-(
      // or use my own override of the dialect... which is not recommended val schema: StructType = getSchema(rs, PostgresDialect, alwaysNullable = true)
      // val schema: StructType = getSchema(rs, JdbcDialects.get("jdbc:postgresql"), alwaysNullable = true)
      val schema: StructType = getSchema(rs, new PostgresDialect) // ??, alwaysNullable = true)

      while ({ rs.next }) {
        val row = new ArrayBuffer[AnyRef]
        var i = 1
        while (i <= columnCount) {
          row += rs.getObject(i)
          i += 1
        }
        val r = Row.fromSeq(row)
        rows += r
      }
      (rows, schema, columnCount)
    } finally {
      // rs.close() // not doing it as I may need to rewind?!
    }

  def resultSet2DataFrame(rs: ResultSet): DataFrame = {
    val (r, sc, _) = resultSet2List(rs)
    spark.createDataFrame(spark.sparkContext.parallelize(r), sc) // we run parallelize to create an RDD[Row]
  }

  // NH to work on... This is not pure btut ojk for this version  0.1

  /**
   * This function is needed as we need to make sure objects of the same name do not clash in the inSessionDB
   * (we can only use the default spark db so this is critical in this version)
   * It assumes a tag to the sql query ... That is all tables default to stitchr_table_name
   * @param q
   * @param dpId
   * @return
   */
  // NH to work on... This is not pure btut ok for this version  0.1
  def query2InSessionRep(q: String, dpId: Int): String = {
    import com.stitchr.core.util.Parse.logicalPlan
    import com.hubspot.jinjava._
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    import scala.collection.JavaConversions._
    // logging.log.info(s"query is $q")
    val t = stripCurlyBrace(q)
    // logging.log.info(s"stripped query is $t")
    val plan = logicalPlan(t)

    // (V0.1) we assume all tables are aliased already so when we replace using jinja we do not need to alias
    val context = plan.collect { case r: UnresolvedRelation => (r.tableName, s"${r.tableName}_${dpId}") }.toMap
    val jinjava = new Jinjava
    val q0 = jinjava.render(q, context)

    // logging.log.info(s"query rewriting $q0")
    q0
  }

  val REGEX_OPEN_CURLY_BRACE = """\{"""
  val REGEX_CLOSED_CURLY_BRACE = """\}"""
  // val REGEX_INLINE_DOUBLE_QUOTES = """\\\"""".r
  //  val REGEX_NEW_LINE = """\\\n""".r
  def stripCurlyBrace(s: String): String =
    s.replaceAll(REGEX_OPEN_CURLY_BRACE, "").replaceAll(REGEX_CLOSED_CURLY_BRACE, "")

  // NH under dev not needed for now
  case class ScColumn(name: String, data_type: String, nullable: String)
  def fromStructType2JsonSchema(s: StructType): Unit =
    // As of Spark 2.4.0, StructField can be converted to DDL format using toDDL method.
    s.toList.foldLeft()(
        (_, next) => {
          val n = ScColumn(next.name, next.dataType.toString, next.nullable.toString).toString
          println(n)
          // println(next.dataType)
          // println(next.nullable)
          //  println(next.toDDL)
        }
    )

}
