package com.stitchr.util.database

// import org.slf4j.{Logger, LoggerFactory}
// jdbc: <sourceType>:<storageType>://<host>:<port>/<database> + user and password from system for now)
case class JdbcProps(
                      dbms: String,
                      driver: String,
                      host: String,
                      port: Int,
                      database: String,
                      dbIndex: String = "jdbc",
                      user: String = null,
                      pwd: String = null,
                      fetchsize: Int = 10000
                    )

trait Jdbc {

  import java.util.Properties
  def jdbcProps: JdbcProps

  /*
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
   */

  val url: String = setupUrl()
  val connectionProperties: Properties = setupConnectionProperties()

  def setupUrl(): String

  def setupConnectionProperties(): Properties
}
