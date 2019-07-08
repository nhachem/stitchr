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

package com.stitchr.core.dbapi

import com.stitchr.util.database.JdbcProps

import java.util.Properties

/* this seems more suitable here. to extend with any dialects we need  */
class Jdbc(jdbcProps: JdbcProps) extends com.stitchr.util.database.JdbcImpl(jdbcProps) {

  /*
  NH: here we register any special dialects that may be supported
  may need to unregister when done... but not really necessary...
   */
  import org.apache.spark.sql.jdbc.JdbcDialects
  /* add here any new dialects what were added to com.stitchr.core.dbapi */
  // Vertica
  JdbcDialects.registerDialect(new VerticaDialect)
  JdbcDialects.registerDialect(new com.stitchr.core.dbapi.PostgresDialect)

}
