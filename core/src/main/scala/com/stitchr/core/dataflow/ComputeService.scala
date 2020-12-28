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

import com.stitchr.core.api.ExtendedDataframe.DataFrameImplicits
import com.stitchr.util.SharedSession.spark
import com.stitchr.core.common.Encoders.{ QueryNode, emptyDependency, _ }
import com.stitchr.core.api.DataSetApi.Implicits
import com.stitchr.util.database.JdbcImpl
import com.stitchr.core.registry.RegistryService.{ getDataPersistence, getDataSet, getObjectRef }
import com.stitchr.core.registry.RegistrySchema.dataSetDF
import com.stitchr.util.Util.time
import com.stitchr.util.EnvConfig.{ appLogLevel, logging }
import com.stitchr.core.util.Parse.{ logicalPlan, getQueryObjectMap, jinjaReplace }
import com.stitchr.core.util.Convert.stripCurlyBrace
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.{ DataFrame, Dataset, Row }

import scala.annotation.tailrec

object ComputeService {

  import spark.implicits._

  /**
   * gets the list of table dependencies by logically parsing the query and extracting the unresolved rel
   * @param queryNode
   * @return
   */
  // case class QueryNode(object_name: String, query: String, mode: String, data_persistence_id: Int)
  def getQueryDependencies(queryNode: QueryNode): DataFrame = {
    import spark.implicits._

    // https://stackoverflow.com/questions/49785796/how-to-get-table-names-from-sql-query
    val dependencySet: DataFrame = queryNode.mode match {
      case "derived" =>
        import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

        // 1st pass into rewrite to strip {{ and }} jinja templates if any. We use Jinja in some cases to rewrite the dependencies but we focus on  dataset.object_ref
        val q: String = stripCurlyBrace(queryNode.query)
        val plan = logicalPlan(q)
        plan
          .collect { case r: UnresolvedRelation => (queryNode.object_ref, r.tableName) } //, queryNode.data_persistence_id) }
          .toDF("object_ref", "dependsOn")

      // NH to revisit in V0.2
      // we should not get here as all are filtered to be derived unless one calls this independently...
      case _ => Array((queryNode.object_ref, queryNode.object_ref)).toSeq.toDF("object_ref", "dependsOn") //, "dataSourceId")

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
    def getDependencySet0(queries: List[QueryNode], df: DataFrame): DataFrame = { //, datasetDF: DataFrame): DataFrame = {
      val dependenciesDF: DataFrame = queries.foldLeft(df)(
          (initial, next) => {
            val df = getQueryDependencies(QueryNode(next.id, next.object_ref, next.object_name, next.query, next.mode, next.data_persistence_id))
            // val df1 = df.join(datasetDF)
            df match {
              case null => initial
              case _ => {
                initial.union(df).distinct
              }
            }
          }
      )
      /*  make it tail recursive until no new objects need to compute dependencies */
      //  TC is intended to be  naive here
      val dependsOnDS = dependenciesDF
        .select("depends_on") //, "data_persistence_id")
        .distinct
        .join(
            dataSetDF,
            dependenciesDF("depends_on") === dataSetDF("object_ref")
        )
        // .filter(s"object_type = '$objectType' and mode in ('derived')")
        .filter(s" mode in ('derived')")
        .select("id", "object_ref", "object_name", "query", "mode", "data_persistence_id")
        .as(queryNodeEncoder)
      // anti dependencies. We pick only the non-base which have not been processed yet and encode as a DataSet(QueryNode)
      val depAnti =
        dependsOnDS
          .join(dependenciesDF.select("object_ref").distinct(), dependsOnDS("object_ref") === dependenciesDF("object_ref"), "left_anti")
          .as(queryNodeEncoder)

      // we could spark Dataset manipulations instead of scala sets by performing a minus operation
      val dep = depAnti.collect.toSet

      // get the ones that were not processed yet and recurse if any
      val queries0 = dep &~ queries.toSet
      dep.size match {
        case 0 => dependenciesDF
        case _ => getDependencySet0(queries0.toList, dependenciesDF) //, datasetDF)
      }
    }
    // initial call into the resursion. Here compute the dataset DF and use it?! I could pass it into the function to reduce the side effect...
    getDependencySet0(queries, Seq.empty[(String, String)].toDF("object_ref", "depends_on")) //, "data_persistence_id"))
    // , datasetDF)

  }

  /**
   * wip: would need to setup the dc_tables at startup or by checking if they ae not available....
   * @param objectReference
   * @return
   */
  def getDataSetQueryNode(objectReference: String): QueryNode = {

    // NH: 2/20/2020 had to replace as databricks was throwing an error with the use of concat
    val sqltest =
      s"""select d.id,
       | d.container || '.' || d.object_name as table_name,
       | d.object_ref,
       | d.container,
       | d.object_name,
       | d.query,
       | d.mode,
       | d.data_persistence_id 
       | from dc_datasets d
       | where 1=1 
       | and d.object_ref in ('$objectReference')""".stripMargin

    val qs = spark
      .sql(
          sqltest
      )
      .as(queryNodeEncoder)
      .collect()

    // if we call on a base object there are no dependencies other than self... so create it here
    // this needs major debugging if we want to generalize
    if (qs.length != 0) qs(0) // assumes the query returns one row back ... note that the object reference should be unique
    else {
      logging.log.warn("queryNode coming back with out a dataset id. Should debug")
      QueryNode(-1, objectReference, objectReference, null, null, -1) // need to fix
    } // NH this is  a BUG 8/1/19. we should always expect one row from this call
  }

  /* this is used to generate DDL for target-based database engines... That is the derived "view" is associated
  with the target engine (and thus all its associated dependencies are on that engine. Not a federated data use case.
   */
  def generateJdbcDDL(referenceObject: String): String = {
    // we will need to reflect object_ref instead of object_name but fine for now
    // forcing a few parameters to filter for the use case
    // NH: 12/18/20 broken... we need to rewrite all dependent objects to use the container.object_name notation
    // this may need jinja rewrite on the dependent objects... unless we override and simplify
    val r = dataSetDF
      .filter(s"object_ref = '$referenceObject' and storage_type = 'database' and mode = 'derived'")
      .select("container", "object_name", "query", "object_type", "schema_id", "data_persistence_id")
      .collect()(0)
    val depMap: Map[String, String] = getQueryObjectMap(r { 2 }.toString)
    val jdbcQuery = jinjaReplace(r { 2 }.toString, depMap)
    val ddl = r(3) match {
      // NH: here we need to build a map and pass the query to jinja. This implimentation assumes all dependencies are in the db...
      case "view" => s"create or replace view ${r(0)}.${r(1)} as $jdbcQuery "
      case "table" =>
        s"create table if not exists ${r(0)}.${r(1)} as $jdbcQuery " // note that we should not use "not exist".. but for now I don't want it to throw an error
    }
    ddl
  }

  // need now to make it recursive over all cases? or just call this function within the compute phase....
  def getRemainingDependencies(derivedSet: Array[Dependency]): Array[Dependency] = {
    val grouped = derivedSet.groupBy(_.object_ref)
    val s: Set[String] = grouped.map(x => (x._1, x._2.length)).filter(_._2 == 1).keySet //map(x => x._1).toSet

    s.foldLeft(Array.empty[Dependency]: Array[Dependency])((_, next) => {
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
  import com.stitchr.core.util.Convert._

  /**
   * This function takes the dependency set and groups by the object_ref counting its dependencies ...
   * It then recursively iterate to establish those objects that have all their dependencies initialized.
   * It stops
   * @param derivedSet array of objects with all its dependencies
   * @return
   */
  def computeDerivedObjects(derivedSet: Array[Dependency]): Row = {

    @tailrec
    def computeDerivedObjects0(derivedSet: Array[Dependency]): Row = {
      // val ds1 = derivedSet.map(p => (p.depends_on -> p))
      // val grouped = derivedSet.groupBy(_.object_name)
      val grouped = derivedSet.groupBy(_.object_ref)
      // note the count(1) has only one entry in the array
      val queries = grouped.map(x => (x._1, x._2, x._2.length)).filter(_._3 == 1).map(x => (x._1, x._2(0)))

      // queries.foreach(p => println(s"query is ${p._1}"))
      // println(s"number of queries is ${queries.size}")

      queries.foldLeft(
          (null, emptyDependency): (String, Dependency)
      )(
          (_, next) => {

            val on = next._1 // also object_name in ._2
            logging.log.info(s"computing $on")

            // NH: 7/8/2019. we need to refactor and use initializeObject?!
            next._2.storage_type match {
              case "file" =>
                spark
                  .sql(query2InSessionRep(next._2.query, next._2.data_persistence_id))
                  .createTemporaryView(next._2.depends_on)
              case "database" => // assume here that we have one target engine with full pushdown (we use straight jdbc)
                val ddl = generateJdbcDDL(next._2.object_ref)
                logging.log.info(s"ddl is ${ddl}")
                // need to use the data source info!!
                val dsn = getDataPersistence(next._2.data_persistence_id)
                import com.stitchr.core.util.Convert._
                // NH: use this to print the case class info
                if (appLogLevel == "INFO") println(cC2Map(dsn))
                val jdbc = new JdbcImpl(dataSourceNode2JdbcProp(dsn))
                println(ddl)
                jdbc.executeDDL(ddl)
                // NH: 6/20/2019.we should add a DF handle to the new views/tables !! Would be perfect to do so based on some parameters(use dataset_state). Code would come here
                val dataSet = getDataSet(next._2.dataset_id)
                // initialize object as it can be used for any type... This is a test. no need for the file url here for now?!
                // NH EXPERIMENTAL... changed from ...
                // initializeJdbcObject(next._2.object_name, null, 1, next._2.schema_id, next._2.data_persistence_id)
                dataSet.initializeJdbcObject
              //dataSet.init

              case _ => spark.sql(next._2.query).createTemporaryView(next._2.object_ref) // to fix
            }
            logging.log.info(s"computed ${next._2.object_ref}")
            next
          }
      )
      val newDerivedSet = getRemainingDependencies(derivedSet)

      val cnt = newDerivedSet.length
      logging.log.info(s"remaining dependency count is $cnt")
      cnt match {
        case 0 => Row() // return empty row if done...
        case _ => computeDerivedObjects0(newDerivedSet)
      }
    }
    logging.log.info(s"calling recursion")
    computeDerivedObjects0(derivedSet)
  }

  def initializeObjects(objectsDS: Dataset[DataSet]): Unit = {
    val tablesArray: Array[DataSet] = objectsDS.collect()
    logging.log.info(s"number of records is ${tablesArray.length}")

    tablesArray.foldLeft()(
        (_, next) => {
          import com.stitchr.core.api.DataSetApi.Implicits
          next.init
          next
        }
    )
  }

  // run the target queries only if storage type is file.
  // for target database connect to the db and verify. will cover in future iterations
  def runQueries(ql: List[String], st: String): Unit =
    st match {
      case "file" => {
        ql.foldLeft()(
            (_, next) => {

              val qr = spark.sql(s"select * from ${getObjectRef(next, st)}") // .cache()
              time(qr.show(false), "running  the query")
              time(println(s"total records returned is ${qr.count()}"), "running the count query")
            }
        )
      }
      case _ => println("querying the db directly is supported but interfaces are not completely developed yet")
    }

}
