package com.stitchr.core.api

import com.stitchr.core.common.Encoders.DataSet
import com.stitchr.core.dataflow.Runner
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.registry.RegistrySchema.dataSourceDF
import com.stitchr.core.registry.RegistryService.{ getDataSource, getSchema }
import com.stitchr.core.util.Convert.dataSourceNode2JdbcProp
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.EnvConfig.{ baseDataFolder, defaultContainer, defaultFileType, defaultWriteMode, sessionRunTime }
import com.stitchr.util.Logging
import com.stitchr.util.Util.time
import org.apache.spark.sql.DataFrame

object DataSet {
  val logging = new Logging

  // is this a hack?! will do for now to add a session_run_time
  // we could impliment as a crossjoin but this may be better.
  import org.apache.spark.sql.functions._
  // fake it and return a value... can use any column
  //def runTimeValue( id: Int): Long = _ => sessionRunTime
  // val sqlValue = udf(runTimeValue(0))
// using a sequence
  import spark.implicits._
  // maybe need to set spark.sql.crossJoin.enabled=true
  val sessionRunTimeAttribute = "session_run_time"
  val df = Seq(sessionRunTime).toDF(sessionRunTimeAttribute)

  /**
   * main save to file function that takes a target object and materializes it in the lake. we use default containers but will modify to use metadata driven from the catalog
   * assumes the objectRef is already in the session catalog
   *
   * @param objectRef
   * @param container
   * @param fileType
   * @return
   */
  // NH 7/25/2019: not urgent but we may be able to make it implicit if a dataset instance object (dataset + timestamp or run_id) can be manipulated inline
  def save2File(
      objectRef: String,
      container: String = defaultContainer,
      fileType: String = defaultFileType
  ): (String, DataFrame) = {
    val fileUrl = s"$container/$objectRef.$fileType"
    // log , s"writing ${fileType} temp checkpoint ${fileOut}")
    logging.log.info(s"fileUrl is $fileUrl for object ref $objectRef")

    // NH: 7/26/2019
    // use default write mode for now...
    //
    val dfExtended = spark.table(objectRef).addrunTimeRef // df.crossJoin(spark.table(objectRef))
    dfExtended.printSchema()
    dfExtended.write.format(fileType).mode(defaultWriteMode).save(fileUrl)
    println(spark.table(objectRef).count)
    (objectRef, spark.read.format(fileType).load(fileUrl))
  }

  implicit class DataFrameImplicits(dataFrame: DataFrame) {
    def addrunTimeRef: DataFrame =
    // trivail match
      dataFrame.schema.fieldNames contains sessionRunTimeAttribute match {
        case true  => dataFrame
        case false => df.crossJoin(dataFrame)
      }

  }

  implicit class Implicits(dataset: DataSet) {

    /**
     * will be used to handle datset instances to be initialized in the runtime spark database (session-based only for now)
     * @param df
     */
    def materializeInSessionDb(df: DataFrame, dataLake: Boolean = false): Unit =
      // NH: 7/25/2019. we should rely on =constructing this on the fly as in init
      // also semantics would be do it if not already instantiated... or maybe this is checked in the init function?
      dataLake match {
        case true  => df.createOrReplaceTempView(s"""dataLake_${dataset.object_ref}""")
        case false => df.createOrReplaceTempView(dataset.object_ref)
      }

    def init: (String, DataFrame) = {
      val objectName = dataset.object_name
      val partitionKey = dataset.partition_key
      val numberOfPartitions = dataset.number_partitions

      val schema = getSchema(dataset.schema_id) // need to trap that there is a real id > 0 ?!

      // NH: 7/25/2019. need to add a check if the table is already instantiated in the session. If yes we will skip and log a warning
      val (datasetDF, viewName) =
        dataset.mode match {
          case "base" =>
            dataset.storage_type match {
              case "file" =>
                val objectURL = baseDataFolder + dataset.query
                dataset.format match {
                  // file formats  have more options. This is a default for now only tested are csv and pipedelimited
                  case "parquet" => (spark.read.format(dataset.format).load(objectURL), dataset.object_name)
                  case "avro"    => (spark.read.format(dataset.format).load(objectURL), dataset.object_name)
                  case "json"    => (spark.read.format(dataset.format).load(objectURL), dataset.object_name)
                  case "orc" =>
                    (spark.read.format(dataset.format).schema(schema).load(objectURL), dataset.object_name)
                  case _ =>
                    // csv files have more option. This is a default ... we need tab vs csv to figure out delimiter
                    val fieldDelimiter = dataset.format match {
                      case "pipeDelimited" => '|'.toString
                      case "tab"           => '\t'.toString
                      case "csv"           => ","
                      case _               => "," // default
                    }

                    (
                        spark.read
                          .schema(schema)
                          .format("csv")
                          .option("header", "false")
                          .option("inferSchema", "false")
                          .option("delimiter", fieldDelimiter)
                          .load(objectURL),
                        // needs to be abstracted as base objects occur as object)name in queries ... s"${dataset.format}_${dataset.id}_$objectName"
                        objectName
                    )
                }
              case "database" =>
                // NH 7/8/2019: assume jdbc and this should be generic to any jdbc source

                val q = s"""select * from ${dataset.object_name}""" // need to fix and use full public.name...
                val dsn = getDataSource(dataSourceDF, dataset.data_source_id)
                val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))

                // NH: 6/27/19. note that we need to close the connection after each initialization?! unless we establish a more global sparkjdbc connection pool
                // we may need to change the use of index which is the dataSourceRef
                // 7/18/2019we could use object_ref instead of constructing the viewName
                (jdbc.readDF(q, partitionKey, numberOfPartitions), s"${dsn.storage_type}_${dsn.id}_$objectName")
            }
          case "derived" =>
            Runner.run(dataset.object_name)
            (spark.table(dataset.object_ref), dataset.object_name)
        }
      // datasetDF.createOrReplaceTempView(viewName)
      materializeInSessionDb(datasetDF)
      (viewName, datasetDF)
    }

    def move2Lake: (String, DataFrame) = {
      init
      materialize
    }

    // NH: 7/18/2019 file materialization only supported for now
    // we chose one target format avro for now
    // NH: 7/25/19 we need to extend to add registration of the generated instance in the catalog
    def materialize: (String, DataFrame) = {
      val (viewName, datasetDF) = dataset.storage_type match {
        case "file" => save2File(dataset.object_name)
        case "database" =>
          save2File(s"${dataset.format}_${dataset.data_source_id}_${dataset.object_name}")
      }
      // side effect we need to replace dataLake with a parameter container
      // materializeInSessionDb(datasetDF, true)
      datasetDF.createOrReplaceTempView(s"""dataLake_$viewName""")
      logging.log.info(s"record count for dataLake_$viewName is ${datasetDF.count()} ")
      (s"dataLake_$viewName", datasetDF)
    }

    // need to work on this.. hacky but maybe a good first version
    def extract2Target: (String, DataFrame) = {
      // assumes the target is the datasource id 1... we need to get it from dataset parameters.
      val dsn = getDataSource(dataSourceDF, 1)
      val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))

      // may not need to specify schema and rely on default schema
      time(
          jdbc.writeTable(spark.table(s"datalake_${dataset.object_ref}").addrunTimeRef, s"${dataset.format}_datalake_${dataset.object_ref}", 2),
          "writing to postgres"
      )

      val (viewName, datasetDF) = dataset.storage_type match {
        case "file" => save2File(dataset.object_name)
        case "database" =>
          save2File(s"${dataset.format}_${dataset.data_source_id}_${dataset.object_name}")
      }
      // side effect we need to replace dataLake with a parameter container
      // materializeInSessionDb(datasetDF, true) //
      datasetDF.createOrReplaceTempView(s"""dataLake_$viewName""")
      (s"dataLake_$viewName", datasetDF)
    }

  }

}
