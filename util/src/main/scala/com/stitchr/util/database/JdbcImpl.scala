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

package com.stitchr.util.database

import java.sql.{ Connection, DatabaseMetaData, DriverManager, ResultSet }
import java.util.Properties

case class JdbcImpl(jdbcProps: JdbcProps) extends Jdbc {
  // class JdbcImpl(jdbcProps: JdbcProps) extends Jdbc {
  // val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  val jdbcUrl: String = setupUrl() // s"jdbc:${jdbcProps.dbms}://${jdbcProps.host}:${jdbcProps.port}/${jdbcProps.database}"

  val conn: Connection = getJdbcConnection(jdbcUrl, jdbcProps.user, jdbcProps.pwd, jdbcProps.driver)

  // metadata ... lazy to be instantiated on first invocation
  // NH: To complete: add PK, indexes and references such as FKs.
  /**
   * databaseMetadata: DatabaseMetaData
   * tablesMetadata: ResultSet
   * columnsMetadata: ResultSet
   * pkMetadata: ResultSet (Primary Key columns)
   * fkMetadata:ResultSet (FK info)
   * uniqueIndexMetadata: ResultSet (Unique Index Info)
   * nonUniqueIndexMetadata: ResultSet (Non Unique Index Info)
   */
  lazy val databaseMetadata: DatabaseMetaData = conn.getMetaData
  lazy val tablesMetadata: ResultSet = getTablesMetadata()
  lazy val columnsMetadata: ResultSet = getColumnsMetadata()
  lazy val pkMetadata: ResultSet = getPkMetadata()
  lazy val fkMetadata: ResultSet = getFkMetadata()
  lazy val uniqueIndexMetadata: ResultSet = getIndexMetadata()
  lazy val nonUniqueIndexMetadata: ResultSet = getIndexMetadata(unique = false)

  def setupUrl(): String =
    s"jdbc:${jdbcProps.dbms}://${jdbcProps.host}:${jdbcProps.port}/${jdbcProps.database}"

  def setupConnectionProperties(): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("driver", jdbcProps.driver)

    // NH: not safe... we need to reconsider
    connectionProperties.put("user", jdbcProps.user)
    connectionProperties.put("password", jdbcProps.pwd)

    // may generalize with a map of key/value pairs
    connectionProperties.put("sslmode", jdbcProps.sslmode)

    connectionProperties
  }

  def getJdbcConnection(url: String, username: String, password: String, driver: String): Connection =
    try {
      Class.forName(driver)
      DriverManager.getConnection(url, username, password)
    } catch { // for now catch everything
      case e: Throwable =>
        e.printStackTrace()
        null
    }

  def executeQuery(query: String): ResultSet =
    conn.createStatement().executeQuery(query)

  def executeDDL(ddl: String): Boolean =
    conn.createStatement().execute(ddl)

  def executeUpdate(sql: String): Int =
    conn.createStatement().executeUpdate(sql)

  def countResultSet(resultset: ResultSet): Long = {
    var cnt = 0
    while (resultset.next()) {
      cnt += 1
    }
    resultset.close()
    cnt
  }

  def getQueryResult(rs: ResultSet): (IndexedSeq[String], Iterator[IndexedSeq[String]]) = {
    val columnCnt: Int = rs.getMetaData.getColumnCount

    val columns: IndexedSeq[String] = 1 to columnCnt map rs.getMetaData.getColumnName
    // get all results
    val queryResults: Iterator[IndexedSeq[String]] = Iterator.continually(rs).takeWhile(_.next()).map { r =>
      columns map r.getString
    }

    (columns, queryResults)
  }

  // metadata
  /*
  ParameterMetaData
  ResultSetMetaData
  DatabaseMetaData
   */

  // NH 4/6 need ot extend and get back result sets and translate to dataframes...

  def getTablesMetadata(
      schemaPattern: String = "%",
      tableNamePattern: String = "%",
      objectTypes: scala.Array[String] = Array("TABLE", "VIEW")
  ): ResultSet =
    databaseMetadata.getTables(conn.getCatalog(), schemaPattern, tableNamePattern, objectTypes)

  def getColumnsMetadata(schemaPattern: String = "%", tableNamePattern: String = "%", columnNamePattern: String = "%"): ResultSet =
    databaseMetadata.getColumns(conn.getCatalog(), schemaPattern, tableNamePattern, columnNamePattern)

  def getPkMetadata(schemaPattern: String = "%", tableNamePattern: String = "%"): ResultSet =
    databaseMetadata.getPrimaryKeys(conn.getCatalog(), schemaPattern, tableNamePattern)

  def getFkMetadata(schemaPattern: String = "%", tableNamePattern: String = "%"): ResultSet =
    databaseMetadata.getExportedKeys(conn.getCatalog(), schemaPattern, tableNamePattern)

  def getIndexMetadata(schemaPattern: String = "%", tableNamePattern: String = "%", unique: Boolean = true, approx: Boolean = true): ResultSet =
    databaseMetadata.getIndexInfo(conn.getCatalog(), schemaPattern, tableNamePattern, unique, approx)

  def closeConnection(): Unit = conn.close()

}
