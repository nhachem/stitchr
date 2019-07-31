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
 */
object EnvConfig {
  val logging = new Logging

  val baseConfigFolder: String = sys.env.getOrElse("baseConfigFolder", System.getProperty("user.dir") + "/config/")
  // val baseFolder: String = "registry"
  val baseRegistryFolder: String = sys.env.getOrElse("baseRegistryFolder", System.getProperty("user.dir") + "/registry/")
  val baseDataFolder: String = sys.env.getOrElse("baseDataFolder", System.getProperty("user.dir") + "/data/")
  // read all config properties.. this needs  work to make it more robust
  val props: Config = readConfig(s"defaults.properties", baseConfigFolder)
  val globalLogging: Boolean = props.getBoolean("global.logging")
  // if we have a postgres catalog initialize the jdbc connection?
  val dataCatalogPersistence: String = props.getString("dc.persistence")
  // if we want hive support set hiveSupport to true
  val hiveSupport: Boolean = props.getBoolean("global.hiveSupport")
  // val globalLogging: Boolean = props.getBoolean("global.logging")

  // need to fix this through config but using system env for now
  val defaultTmpContainer = props.getString("global.defaultTmpContainer") // sys.env.getOrElse("defaultTmpContainer", System.getProperty("global.defaultTmpContainer"))
  val defaultContainer = props.getString("global.defaultContainer") // sys.env.getOrElse("defaultContainer", System.getProperty("global.defaultContainer"))
  val defaultFileType = props.getString("global.defaultFileType") // sys.env.getOrElse("defaultFileType", System.getProperty("global.defaultFileType"))
  val defaultWriteMode: String = props.getString("global.defaultWriteMode")

  val sessionRunTime: Long = System.nanoTime() // default long to use as a reference to identify all data for this session. This is incrementing but set once per session

  logging.log.info(s"default write mode is $defaultWriteMode")
  logging.log.info(s"session run time is $sessionRunTime")
  logging.log.info(s"DC persistence source is $dataCatalogPersistence")
  logging.log.info(s"Default TMPContainer is $defaultTmpContainer")
  logging.log.info(s"Default file type is $defaultFileType")
}
