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

import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.registry.RegistryService.{getJsonSchema, putJsonSchema, getSchemaArray}
import com.stitchr.util.EnvConfig.logging
import com.stitchr.util.JsonConverter._
import com.stitchr.core.common.Encoders.SchemaColumn
import org.apache.spark.sql.Encoder

spark.sparkContext.setLogLevel("INFO")

// just list the session info
val configMap:Map[String, String] = spark.conf.getAll

case class Test(id: Int, object_ref: String, format: String)
val testEncoder: Encoder[Test] = org.apache.spark.sql.Encoders.product[Test]

val j = """[{"id":-1,"object_ref":"date_dim0_1","format":"postgresql"}, {"id":-1,"object_ref":"date_dim0_1","format":"postgresql"}]"""

import spark.implicits._
fromJson[Array[Test]](j).toIndexedSeq.toDF().as[Test].show(false)


getJsonSchema(1)

val ds = fromJson[Array[SchemaColumn]](getJsonSchema(1)).toList.toDF.as[SchemaColumn]

val testJson  = toJson[Array[SchemaColumn]](ds.map(r => r.copy(id = 100)).collect())

putJsonSchema(testJson)
