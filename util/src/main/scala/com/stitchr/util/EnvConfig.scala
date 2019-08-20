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

package com.stitchr.util

import com.stitchr.util.Properties.readConfig
import com.typesafe.config.Config

/**
 * common globals and env parameters are managed here
 *  we may move under sparkutils
 *
 */
object EnvConfig {
  val logging = new Logging

  /**
   * baseConfigFolder is the location of the config file coming from the associated environment variable see env.sh
   */
  val baseConfigFolder: String = sys.env.getOrElse("baseConfigFolder", System.getProperty("user.dir") + "/config/")
  /**
    * baseRegistryFolder is the location of the registry files if we use a file-based registry (for dev purposes and demo only). see env.sh
    */
  val baseRegistryFolder: String = sys.env.getOrElse("baseRegistryFolder", System.getProperty("user.dir") + "/registry/")
  /**
    * baseDataFolder is the location of the data files if we use a file-based registry (for dev purposes and demo only). see env.sh determine its use with overrideDefaultContainer
    */
  val baseDataFolder: String = sys.env.getOrElse("baseDataFolder", System.getProperty("user.dir") + "/data/")

  /**
  * read all config properties in props. It assumes a default.properties file in the baseConfigFolder
    */
  val props: Config = readConfig(s"defaults.properties", baseConfigFolder)
  val globalLogging: Boolean = props.getBoolean("global.logging")
  // if we have a postgres catalog initialize the jdbc connection?
  /**
  * a value of dc means a postgres-based data catalog
    */
  val dataCatalogPersistence: String = props.getString("dc.persistence")
  // if we want hive support set hiveSupport to true
  val hiveSupport: Boolean = props.getBoolean("global.hiveSupport")
  // val globalLogging: Boolean = props.getBoolean("global.logging")

  // need to fix this through config but using system env for now
  val defaultTmpContainer: String = props.getString("global.defaultTmpContainer")
  val defaultContainer: String = props.getString("global.defaultContainer")
  val defaultFileType: String = props.getString("global.defaultFileType")
  val defaultWriteMode: String = props.getString("global.defaultWriteMode")

  val logLevel: String = props.getString("spark.logLevel")
  val appLogLevel: String = props.getString("app.logLevel")

  /**
  * each processed object gets a run_time_ref log column associated with each record if addRunTImeRef is true
    */
  val addRunTimeRef: Boolean = props.getBoolean("global.addRunTimeRef")
  val sessionRunTime: Long = System.nanoTime()

  logging.log.info(s"default write mode is $defaultWriteMode")
  logging.log.info(s"session run time is $sessionRunTime")
  logging.log.info(s"DC persistence source is $dataCatalogPersistence")
  logging.log.info(s"Default TMPContainer is $defaultTmpContainer")
  logging.log.info(s"Default file type is $defaultFileType")

  val cloudStorage: Boolean = props.getBoolean("global.cloudStorage")

  // NH EXPERIMENTAL 8/9/2019
  val globalTempDbEnabled: Boolean = props.getBoolean("global.globalTempDbEnabled")
  logging.log.info(s"global_temp database enabled is $globalTempDbEnabled")

  // val inSessionDB: String = { if (globalTempDbEnabled) "global_temp." else ""}
  /**
  * must be set to true to use the metadata associated with datasets
    */
  val overrideDefaultContainer: Boolean = props.getBoolean("global.overrideDefaultContainer")
  logging.log.info(s"destination persistence is associated with the dataset $overrideDefaultContainer")

  // NH 8/1/2019 EXPERIMENTAL: adding threading support ... may change to using cats library and or scala Future
  // val threadCount: Int = props.getInt("concurrent.threadcount")
  val semaphores: Int = props.getInt("concurrent.semaphores")
  val sem: java.util.concurrent.Semaphore = new java.util.concurrent.Semaphore(semaphores)
}
