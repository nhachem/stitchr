package com.stitchr.util.database

import org.apache.spark.sql.types._

object Schema {

  // schema stuff
  case class SchemaType(dataType: org.apache.spark.sql.types.DataType, nullable: Boolean)

  /**
   * used to build a mapping from different schema data sources... such as postgres, vertica, spark....
   * all mapping are into spark.
   * this is not complete and will be expanded as needed
   * * NH this is a subset of all possible types... best to get the list from
   * * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-DataType.html
   * * or
   * * https://spark.apache.org/docs/latest/sql-programming-guide.html#data-types
   * * and
   * * https://github.com/apache/spark/tree/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types
   *
   * @param stype type to map to SparkSQL Type
   * @param precision used for decimal types
   * @param scale used for decimal types
   * @return
   */
  def toSqlType(stype: String, precision: Int, scale: Int): SchemaType =
    stype match {
      case "varchar"                => SchemaType(IntegerType, nullable = false)
      case "serial"                 => SchemaType(IntegerType, nullable = false)
      case "integer"                => SchemaType(IntegerType, nullable = false)
      case "string"                 => SchemaType(StringType, nullable = false)
      case "character varying"      => SchemaType(StringType, nullable = false)
      case "boolean"                => SchemaType(BooleanType, nullable = false)
      case "bool"                   => SchemaType(BooleanType, nullable = false)
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

}
