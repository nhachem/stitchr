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

  val baseConfigFolder: String = sys.env.getOrElse("baseConfigFolder", System.getProperty("user.dir") + "/config/")
  // val baseFolder: String = "registry"
  val baseRegistryFolder: String = sys.env.getOrElse("baseRegistryFolder", System.getProperty("user.dir") + "/registry/")
  val baseDataFolder: String = sys.env.getOrElse("baseDataFolder", System.getProperty("user.dir") + "/data/")
  // read all config properties.. this needs  work to make it more robust
  val props: Config = readConfig(s"defaults.properties", baseConfigFolder)
  // if we have a postgres catalog initialize the jdbc connection?
  val dataCatalogPersistence = props.getString("dc.persistence")
  // if we want hive support set hiveSupport to true
  val hiveSupport = props.getBoolean("global.hiveSupport")
}
