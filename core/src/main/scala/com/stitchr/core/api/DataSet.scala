package com.stitchr.core.api

import com.stitchr.core.common.Encoders.DataSet
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.core.registry.RegistrySchema.dataSourceDF
import com.stitchr.core.registry.RegistryService.{ getDataSource, getSchema }
import com.stitchr.core.util.Convert.dataSourceNode2JdbcProp
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.EnvConfig.{ baseDataFolder, defaultContainer, defaultFileType }
import com.stitchr.util.Logging
import org.apache.spark.sql.DataFrame

object DataSet {
  val logging = new Logging

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

    // NH: 7/18/2019
    // default to overrite mode for now
    spark.table(objectRef).write.format(fileType).mode("overwrite").save(fileUrl)
    println(spark.table(objectRef).count)
    (objectRef, spark.read.format(fileType).load(fileUrl))
  }

  implicit class Implicits(dataset: DataSet) {

    /**
     * will be used to handle datset instances to be initialized in the runtime spark database (session-based only for now)
     * @param df
     */
    def materializeInInSessionDb(df: DataFrame): Unit =
      // NH: 7/25/2019. we should rely on =constructing this on the fly as in init
      // also semantics would be do it if not already instantiated... or maybe this is checked in the init function?
      df.createOrReplaceTempView(dataset.object_ref)

    def init: (String, DataFrame) = {
      val objectName = dataset.object_name
      val objectURL = baseDataFolder + dataset.query
      val partitionKey = dataset.partition_key
      val numberOfPartitions = dataset.number_partitions

      val schema = getSchema(dataset.schema_id) // need to trap that there is an real id > 0 ?!

      // NH: 7/25/2019. need to add a check if the table is already instantiated in the session. If yes we will skip and log a warning
      val (datasetDF, viewName) = dataset.storage_type match {
        case "file" =>
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

      datasetDF.createOrReplaceTempView(viewName)
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
      datasetDF.createOrReplaceTempView(s"""dataLake_$viewName""")
      (s"dataLake_$viewName", datasetDF)
    }

  }

}
