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

import com.stitchr.core.common.Encoders.{ datasetEncoder, extendedDependencyEncoder }
import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.dataflow.ComputeService.{ computeDerivedObjects, getDependencySet, initializeObjects }
import com.stitchr.core.registry.RegistryService.initializeDataCatalogViews
import com.stitchr.util.Util.time
import com.stitchr.util.Logging
import com.stitchr.core.registry.RegistrySchema.datasetDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * used to run a derivation of a query
 * 1. initialize the datacatlog views
 * 2.
 */
object Runner {
  val logging = new Logging

  /**
   *
   * @param objectReference
   * @return SparkSession is not used directly, but we need if we implement multiple sessions and to interface properly with Python
   */
  def run(objectReference: String): SparkSession = { // }, storageType: String): SparkSession = {

    println(spark.conf.getAll)

    // initialize 2 dc_ views
    initializeDataCatalogViews()

    // val datasetDF = spark.sql(s"select * from dc_datasets where storage_type = '$storageType'")
    // val datasetDF = spark.sql(s"select * from dc_datasets")

    // using DataFrame is handy but has a lot of overhead
    val depSet = getDependencySet(List(getDependencySet(objectReference)))

    // NH: just used if we need to debug or showcase
    // depSet.show().toString

    // finally add self dependencies so that we can run asynchronously
    /**
     * targets will be file, database or any
     * case target is file then we keep the base objects and add the self dependencies
     * case database then we drop the base dependencies (as we assume they are already instantiated) and add the self dependencies
     * case mixed (any) we drop all base dependencies that are  db objects and add the self dependencies
     * Note that in the any/mixed situation, optimization is tricky and may need special coding
     * as the spark optimizer can do push down but is not mature yet and any sql across engines will need special analysis
     */
    // need to alias to disambiguate column names
    val df1 = depSet.select("object_name").distinct.as("left")
    val df2 = depSet.select("object_name", "data_source_id").distinct.toDF("depends_on", "data_source_id") // adding data_source_id
    val selfDS = df1.join(df2, df1("object_name") === df2("depends_on"))

    // this is the whole dependency graph covered in a table of edges
    val dependencyGraphDF = depSet.union(selfDS).distinct()

    dependencyGraphDF.foreach(r => logging.log.info(s"dependency is $r"))

    dependencyGraphDF.show(false)

    // dependencyGraphDF.printSchema()

    /**
     * now to run/derive the query we follow this process
     * 1. initialize all base object and "delete" from the dependency graph
     * 2. iterate or recurse
     *   pick all objects that have only self references and evaluate them (all their dependencies are covered)
     *   delete those from the dependency graph by delete all depends_on in (compute views)
     *   recurse until the dependency graph is an empty set and so all dependencies and original query has been derived
     */
    // step 1 initialize all base objects
    // select depends_on from dependency_graph join with datasets where mode = 'base'
    // in this system datasets are already filtered on files only

    //NH IMPORTANT (BUG): usisng data_source_id disables cross file storage containers (and this is not a good idea... this is a BUG!
    // works if we do not have a fully federated environment
    val baseObjectsDF = datasetDF
      .filter(s"mode = 'base'") // and storage_type = '$storageType' ")
      .join(
          dependencyGraphDF,
          datasetDF("object_name") === dependencyGraphDF("depends_on") and datasetDF("data_source_id") === dependencyGraphDF("data_source_id"),
          "leftsemi"
      ) // NH IMPORTANT: here we need to also include the  data_source_id in the join...
      .select(
          "id",
          "object_ref", // NH: 7/10/2019... need to deprecate the use of object_ref...
          "format",
          "storage_type",
          "mode",
          "container",
          "object_type",
          "object_name",
          "query",
          "partition_key",
          "number_partitions",
          "priority_level",
          "dataset_state_id",
          "schema_id",
          "data_source_id",
          "data_destination_id"
      )
      .as(datasetEncoder)

    // initializes objects as views...
    // we may need to extend to return a Map of DataFrame references (object_name --> dataFrame)
    initializeObjects(baseObjectsDF)

    spark.catalog.listTables.show(50)

    // delete base objects as they were initialized
    val derivedDF = dependencyGraphDF
      .join(baseObjectsDF, dependencyGraphDF("depends_on") === baseObjectsDF("object_name"), "left_anti")
    derivedDF.show(false)

    // step 2 recursively process derived objects
    // initialize by running query and associating a view...
    // recursively until all dependencies are consumed (assume all are files)

    import org.apache.spark.sql.functions._
    val dfl = derivedDF.as("dfl")
    val dfr = datasetDF.as("dfr")
    val derivedDependencyQueriesDS = dfl
      .join(dfr, dfl("depends_on") === dfr("object_name") and dfl("data_source_id") === dfr("data_source_id"))
      .select(col("dfl.object_name"), col("dfl.depends_on"), col("dfr.storage_type"), col("dfr.query"), col("dfr.schema_id"), col("dfr.data_source_id"))
      .as(extendedDependencyEncoder)

    // testing the derivation
    // get results
    val derivedSet = derivedDependencyQueriesDS.collect

    //   time(computeDerivedObjects(derivedSet, storageType), "timing the run")
    time(computeDerivedObjects(derivedSet), "timing the run")
    spark
  }

}
