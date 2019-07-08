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

package com.stitchr.core.dbapi

/* NH  9/11/18
 * need proper data type mappings for vertica jdbc and vertica types
 * upon use class instance is registered as a JdbcDialect using something like
 * val dialect = new VerticaDialect JdbcDialects.registerDialect(dialect)
 */

import java.sql.Types

import org.apache.spark.sql.jdbc.{ JdbcDialect, JdbcType }
import org.apache.spark.sql.types._

// references to the basis of this is https://akashrehan.wordpress.com/2018/01/22/spark-dialect-for-datatype-conversion/
// and https://developer.ibm.com/code/2017/11/29/customize-spark-jdbc-data-source-work-dedicated-database-dialect/
class VerticaDialect extends JdbcDialect {

  // NH val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:vertica")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
    typeName match {
      case "Varchar" => Some(StringType)
      case "Integer" => Some(FloatType)
      case _         => None
    }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType  => Option(JdbcType("VARCHAR", Types.VARCHAR))
    case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
    case LongType    => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
    case DoubleType  => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
    case FloatType   => Option(JdbcType("REAL", java.sql.Types.FLOAT))
    case ShortType   => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
    case ByteType    => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
    case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
    //  case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
    case BinaryType     => Option(JdbcType("BLOB", java.sql.Types.BLOB))
    case TimestampType  => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
    case DateType       => Option(JdbcType("DATE", java.sql.Types.DATE))
    case t: DecimalType => Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
    case _              => None
  }

}
