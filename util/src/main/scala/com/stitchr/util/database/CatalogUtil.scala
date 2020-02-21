package com.stitchr.util.database

import com.stitchr.util.SharedSession.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalog.Table

object CatalogUtil {

  val catTables: Dataset[Table] = spark.catalog.listTables

  def infoListTables(numRows: Int = 100, truncate: Boolean = false): Unit = catTables.show(numRows, truncate)

  def infoListTablesCount: Long = catTables.count()

}
