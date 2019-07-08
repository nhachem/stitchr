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

package com.stitchr.core.dataflow

import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.util.EnvConfig._
import com.stitchr.core.common.Encoders._
import com.stitchr.core.dbapi.SparkJdbcImpl
import com.stitchr.util.database.JdbcImpl
import com.stitchr.core.registry.RegistryService.{ getDataSource, getSchema }
import com.stitchr.core.registry.RegistrySchema.datasetDF
import com.stitchr.core.registry.RegistrySchema.dataSourceDF
import com.stitchr.core.util.Convert.{ config2JdbcProp, dataSourceNode2JdbcProp }
import org.apache.spark.sql.{ DataFrame, Dataset, Row }

import scala.annotation.tailrec

object ComputeService {

  import spark.implicits._

  /**
   * gets the list of table dependencies by logically parsing the query and extracting the unresolved relations
   *
   * @param sqlString
   * @param objectName
   * @return
   */
  def getQueryDependencies(sqlString: String, objectName: String, mode: String = "derived"): DataFrame = {
    import spark.implicits._

    // https://stackoverflow.com/questions/49785796/how-to-get-table-names-from-sql-query
    val dependencySet = mode match {
      case "derived" =>
        import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
        import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
        val plan: LogicalPlan = spark.sessionState.sqlParser.parsePlan(sqlString)

        //import spark.implicits._
        /*
        NH: This is critical to fix later... what comes back from the parse is typically container.object_name
        but the keying in the dataset is on object_name only.... We need to include the format and container
         */
        val dependsOn = plan.collect { case r: UnresolvedRelation => r.tableName }.toDF("dependsOn")
        List(objectName).toDF("objectName").crossJoin(dependsOn)

      // we should not get here as all are filtered to be derived unless one calls this independently...
      case _ => Array((objectName, objectName)).toSeq.toDF("objectName", "dependsOn")

    }
    dependencySet.distinct // distinct is needed as dependencies may have multiple occurrences in the logical plan
  }

  /**
   * getDependencySet computes the full dependency graph. we may need to filter on different cases....
   * @param queries
   * @return
   */
  // def getDependencySet(queries: List[QueryNode], objectType: String = "file"): DataFrame = {
  def getDependencySet(queries: List[QueryNode]): DataFrame = {
    @tailrec
    def getDependencySet0(queries: List[QueryNode], df: DataFrame, datasetDF: DataFrame): DataFrame = {
      val dependenciesDF: DataFrame = queries.foldLeft(df)(
          (initial, next) => {
            val df = getQueryDependencies(next.query, next.object_name, next.mode)
            df match {
              case null => initial
              case _    => initial.union(df).distinct
            }
          }
      )
      /*  make it tail recursive until no new objects need to compute dependencies */
      //  TC is intended to be  naive here
      val dependsOnDS = dependenciesDF
        .select("depends_on")
        .distinct
        .join(datasetDF, dependenciesDF("depends_on") === datasetDF("object_name"))
        // .filter(s"object_type = '$objectType' and mode in ('derived')")
        .filter(s" mode in ('derived')")
        .select("query", "object_name", "mode")
        .as(queryNodeEncoder)
      // anti dependencies. We pick only the non-base which have not been processed yet and encode as a DataSet(QueryNode)
      val depAnti =
        dependsOnDS
          .join(dependenciesDF.select("object_name").distinct(), dependsOnDS("object_name") === dependenciesDF("object_name"), "left_anti")
          .as(queryNodeEncoder)

      // we could spark Dataset manipulations instead of scala sets by performing a minus operation
      val dep = depAnti.collect.toSet

      // get the ones that were not processed yet and recurse if any
      val queries0 = dep &~ queries.toSet
      dep.size match {
        case 0 => dependenciesDF
        case _ => getDependencySet0(queries0.toList, dependenciesDF, datasetDF)
      }
    }

    // initial call into the resursion. Here compute the dataset DF and use it?! I could pass it into the function to reduce the side effect...
    getDependencySet0(queries, Seq.empty[(String, String)].toDF("object_name", "depends_on"), datasetDF)

  }

  /**
   * wip: would need to setup the dc_tables at startup or by checking if they ae not available....
   * @param objectReference
   * @return
   */
  def getDependencySet(objectReference: String): QueryNode = {

    // 2.4.3 val sqltext = s"select container || '.' || object_name as table_name, container, object_name, query, mode from dc_datasets where object_name in ('$objectReference')"
    // in 2.2.3 || is not supported in sparkSQL
    val sqltest =
      s"select concat(container, '.',  object_name) as table_name, container, object_name, query, mode from dc_datasets where object_name in ('$objectReference')"
    val qs = spark
      .sql(
          sqltest
      )
      .as(queryNodeEncoder)
      .collect()

    // if we call on a base object there are no dependencies othetr than self... so create it here
    // this needs major debugging if we want to generalize
    if (qs.length != 0) qs(0) // assumes the query returns one row back ... note that the object reference should be unique
    else QueryNode(objectReference, null, null)
  }

  def generateDDL(referenceObject: String): String = {
    // we will need to reflect object_ref instead of object_name but fine for now
    // forcing a few parameters to filter for the use case
    val r = datasetDF
      .filter(s"object_name = '$referenceObject' and storage_type = 'database' and mode = 'derived'")
      .select("container", "object_name", "query", "object_type", "data_source_ref")
      .collect()(0)
    val ddl = r(3) match {
      case "view" => s"create or replace view ${r(0)}.${r(1)} as ${r(2)} "
      case "table" =>
        s"create table if not exists ${r(0)}.${r(1)} as ${r(2)} " // note that we should not use "not exist".. but for now I want it to throw an error
    }
    ddl
  }

  // need now to make it recursive over all cases? or just call this function within the compute phase....
  def getRemainingDependencies(derivedSet: Array[ExtendedDependency]): Array[ExtendedDependency] = {
    val grouped = derivedSet.groupBy(_.object_name)
    val s: Set[String] = grouped.map(x => (x._1, x._2.length)).filter(_._2 == 1).keySet //map(x => x._1).toSet

    s.foldLeft(Array.empty[ExtendedDependency]: Array[ExtendedDependency])((_, next) => {
      derivedSet
        .map(p => p.depends_on -> p)
        .filter(
            p =>
              p._1 match {
                case `next` => false
                case _      => true
            }
        )
        .map(p => p._2)
    })
  }

  def computeDerivedObjects(derivedSet: Array[ExtendedDependency], target: String = "file"): Row = {

    @tailrec
    def computeDerivedObjects0(derivedSet: Array[ExtendedDependency]): Row = {
      // val ds1 = derivedSet.map(p => (p.depends_on -> p))
      val grouped = derivedSet.groupBy(_.object_name)
      // note the count(1) has only one entry in the array
      val queries = grouped.map(x => (x._1, x._2, x._2.length)).filter(_._3 == 1).map(x => (x._1, x._2(0)))

      // queries.foreach(p => println(s"query is ${p._1}"))
      // println(s"number of queries is ${queries.size}")

      queries.foldLeft((null, ExtendedDependency(null, null, null, null)): (String, ExtendedDependency))(
          (_, next) => {

            val on = next._1 // also object_name in ._2
            println(s"computing $on")

            // NH: 7/8/2019. we need to refactor and extend and use initializeObject?!
            target match {
              case "file" => spark.sql(next._2.query).createOrReplaceTempView(next._2.object_name)
              case "database" => // assume here that we have one target engine with full pushdown (we use straight jdbc)
                val ddl = generateDDL(next._2.object_name)
                // need to use the data source info!!
                val dsn = getDataSource(dataSourceDF, next._2.data_source_ref)
                val jdbc = new JdbcImpl(dataSourceNode2JdbcProp(dsn))

                jdbc.executeDDL(ddl)
                // NH: 6/20/2019.we should add a DF handle to the new views/tables !! Would be perfect to do so based on some parameters(use dataset_state). Code would come here

                // initialize object as it can be used for any type... This is a test. no need for the file url here for now?!
                initializeJdbcObject(next._2.object_name, null, 1, next._2.data_source_ref)

              case _ => spark.sql(next._2.query).createOrReplaceTempView(next._2.object_name) // to fix
            }
            println(s"computed ${next._2.object_name}")
            next
          }
      )
      val newDerivedSet = getRemainingDependencies(derivedSet)

      val cnt = newDerivedSet.length
      println(s"remaining dependency count is $cnt")
      // println(newDerivedSet.mkString)
      cnt match {
        case 0 => Row() // return empty row if done...
        case _ => computeDerivedObjects0(newDerivedSet)
      }
    }
    println("calling recursion")
    computeDerivedObjects0(derivedSet)
  }

  def initializeObjects(objectsDS: Dataset[DataSet]): Unit = {
    val tablesArray = objectsDS.collect()
    println(s"number of records is ${tablesArray.length}")

    tablesArray.foldLeft()(
        (_, next) => {
          initializeObject(next)
          next
        }
    )
  }

  /**
   * just working with files and assuming target spark for now
   *
   * @param objectName
   * @param partitionKey: String,
   * @param numberOfPartitions: Int = 4,
   * @param dataSourceRef: String
   * @return
   */
  // NH 7/8/2018need to fix the api. for databases with jdbc, we can use one entry point... This is just needed in the execute DDL code.... will refactor
  def initializeJdbcObject(
      objectName: String,
      partitionKey: String,
      numberOfPartitions: Int = 4,
      dataSourceRef: String
  ): DataFrame = {

    val schema = getSchema(objectName) // we used objectName and schema_ref as the same value for now but not quite correct
    val (dataset, viewName) = {
      val q = s"""select * from $objectName""" // need to fix and use full public.name...
      val dsn = getDataSource(dataSourceDF, dataSourceRef)
      val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))

      // NH: 6/27/19. note that we need to close the connection after each initialization?! unless we establish a more global sparkjdbc connection pool
      // we may need to change the use of index which is the dataSourceRef
      (jdbc.readDF(q, partitionKey, numberOfPartitions), s"${dsn.index}_$objectName")
    }

    dataset.createOrReplaceTempView(viewName)
    dataset
  }

  /**
   * just working with files and assuming target spark for now
   * @param dataset
   * @return

   * this initializes the object in the db and returns also a DataFrame handle for the object
   * NH 7/8/2018. we need to look at getting baseDataFolder from the metadata. Kept iyt like that for now as we use the tool with a local root folder
   */
  def initializeObject(dataset: DataSet): DataFrame = { //, schemaColumn: SchemaColumn, dataSource: DataSource): DataFrame = {

    val objectName = dataset.object_name
    val objectURL = baseDataFolder + dataset.query
    val partitionKey = dataset.partition_key
    val numberOfPartitions = dataset.number_partitions

    val schema = getSchema(dataset.schema_ref) // we used objectName and schema_ref as the same value for now but not quite correct

    val (datasetDF, viewName) = dataset.storage_type match {
      case "file" =>
        dataset.format match {
          // file formats  have more options. This is a default for now only tested are csv and pipedelimited
          case "parquet" => (spark.read.format(dataset.format).load(objectURL), dataset.object_name)
          case "avro"    => (spark.read.format(dataset.format).load(objectURL), dataset.object_name)
          case "orc" => {
            (spark.read.format(dataset.format).schema(schema).load(objectURL), dataset.object_name)
          }
          case _ => {
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
                objectName
            )
          }
        }

      case "database" => {
        // NH 7/8/2019: assume jdbc and this should be generic to any jdbc source

        val q = s"""select * from ${dataset.object_name}""" // need to fix and use full public.name...
        val dsn = getDataSource(dataSourceDF, dataset.data_source_ref)
        val jdbc = SparkJdbcImpl(dataSourceNode2JdbcProp(dsn))

        // NH: 6/27/19. note that we need to close the connection after each initialization?! unless we establish a more global sparkjdbc connection pool
        // we may need to change the use of index which is the dataSourceRef
        (jdbc.readDF(q, partitionKey, numberOfPartitions), s"${dsn.index}_$objectName")
      }
    }

    datasetDF.createOrReplaceTempView(viewName)
    datasetDF
  }

}
