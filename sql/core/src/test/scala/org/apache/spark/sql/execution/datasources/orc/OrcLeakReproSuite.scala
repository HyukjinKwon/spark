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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{DebugFilesystem, SparkException}
import org.apache.spark.sql.execution.datasources.SchemaMergeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * THROWAWAY suite to reproduce the OrcSource leaked-file-stream flake.
 *
 * Repeats the exact `SPARK-11412 read and merge orc schemas in parallel`
 * fail-fast sub-case (ignoreCorruptFiles=false, which intentionally aborts a
 * Spark job) many times, checking `DebugFilesystem.assertNoOpenStreams()`
 * after each iteration. If the aborted parallel schema read orphans a stream,
 * this reproduces "There are N possibly leaked file streams" far more reliably
 * than the single occurrence in the real suite.
 */
class OrcLeakReproSuite extends SharedSparkSession {
  import testImplicits._

  private def mergeWithCorruptFileFailFast(): Unit = {
    withSQLConf(
      SQLConf.IGNORE_CORRUPT_FILES.key -> "false",
      SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      withTempDir { dir =>
        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sessionState.newHadoopConf())
        val basePath = dir.getCanonicalPath
        val path1 = new Path(basePath, "first")
        val path2 = new Path(basePath, "second")
        val path3 = new Path(basePath, "third")
        // Widen the race: several valid ORC files + one corrupt (JSON) file.
        spark.range(0, 8).toDF("a").repartition(8).write.orc(path1.toString)
        spark.range(8, 16).toDF("b").repartition(8).write.orc(path2.toString)
        spark.range(2, 3).toDF("a").coalesce(1).write.json(path3.toString)
        val fileStatuses =
          Seq(fs.listStatus(path1), fs.listStatus(path2), fs.listStatus(path3)).flatten
        intercept[SparkException] {
          SchemaMergeUtils.mergeSchemasInParallel(
            spark, Map.empty, fileStatuses, OrcUtils.readOrcSchemasInParallel)
        }
      }
    }
  }

  test("SPARK-XXXXX repro: merge-schema abort must not leak file streams") {
    val iterations = sys.env.getOrElse("ORC_REPRO_ITERS", "200").toInt
    var leakedAt = -1
    var i = 0
    while (i < iterations && leakedAt < 0) {
      DebugFilesystem.clearOpenStreams()
      mergeWithCorruptFileFailFast()
      // Do NOT use eventually here; we want to observe the transient leak
      // exactly like afterEach would, but immediately, to measure raw rate.
      try {
        DebugFilesystem.assertNoOpenStreams()
      } catch {
        case e: IllegalStateException =>
          // Give async close a brief chance, mirroring afterEach's tolerance.
          Thread.sleep(50)
          try {
            DebugFilesystem.assertNoOpenStreams()
          } catch {
            case _: IllegalStateException =>
              leakedAt = i
              // scalastyle:off println
              println(s"LEAK REPRODUCED at iteration $i: ${e.getMessage}")
              // scalastyle:on println
          }
      }
      i += 1
    }
    // scalastyle:off println
    println(s"ORC_REPRO done: iterations=$i leakedAt=$leakedAt")
    // scalastyle:on println
  }
}
