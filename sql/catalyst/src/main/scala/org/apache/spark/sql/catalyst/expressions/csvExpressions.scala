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

package org.apache.spark.sql.catalyst.expressions

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.sql.catalyst.csv._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function infers schema of CSV string.
 */
@ExpressionDescription(
  usage = "_FUNC_(csv[, options]) - Returns schema in the DDL format of CSV string.",
  examples = """
    Examples:
      > SELECT _FUNC_('1,abc');
       struct<_c0:int,_c1:string>
  """,
  since = "3.0.0")
case class SchemaOfCsv(child: Expression)
  extends UnaryExpression with String2StringExpression with CodegenFallback {

  override def convert(v: UTF8String): UTF8String = {
    val parsedOptions = new CSVOptions(Map.empty, true, "UTC")
    val parser = new CsvParser(parsedOptions.asParserSettings)
    val row = parser.parseLine(v.toString)

    if (row != null) {
      val header = row.zipWithIndex.map { case (_, index) => s"_c$index" }
      val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
      val fieldTypes = CSVInferSchema.inferRowType(parsedOptions)(startType, row)
      val st = StructType(CSVInferSchema.toStructFields(fieldTypes, header, parsedOptions))
      UTF8String.fromString(st.catalogString)
    } else {
      null
    }
  }
}
