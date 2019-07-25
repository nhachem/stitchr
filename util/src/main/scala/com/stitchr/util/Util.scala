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

import com.stitchr.util.EnvConfig.globalLogging
import org.slf4j.{ LoggerFactory, Logger }

object Util {
  // may use or just have a logger class used across
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  // shamelessly adapted from http://stackoverflow.com/questions/9160001/how-to-profile-methods-in-scala
  // has a side effect
  def time[R](block: => R, message: String): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    globalLogging match {
      case true => logger.info(s"$message, Elapsed time: " + (t1 - t0) / 1000000 + "ms")
      case false =>  println(s"$message Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    }
    result
  }

  // seldom useful. mostly when function do not return a result...
  def getRunTime[R](block: => R ): Long = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val elapsed: Long = (t1 - t0)/1000000
    println("elapsed time is " + elapsed.toString)
    elapsed
  }

  // attaches the runtime to the result and returns a tuple
  def annotateRunTime[R](block: => R , message: String): (R, Long)  = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    (result, (t1 - t0)/1000000)
  }

}
