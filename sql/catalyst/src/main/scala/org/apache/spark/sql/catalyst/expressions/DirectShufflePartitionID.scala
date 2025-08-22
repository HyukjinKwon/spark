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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, DataType, LongType}

/**
 * Returns raw partition ID.
 */
case class DirectShufflePartitionID(override val child: Expression)
    extends UnaryExpression with ExpectsInputTypes {

  override def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def nullable: Boolean = false

  override def dataType: DataType = child.dataType

  override def nodeName: String = "direct_shuffle_partition_id"

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) {
      throw QueryCompilationErrors.nullArgumentError(nodeName, "expr")
    }
    result
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val code = childGen.code + code"""
      if (${childGen.isNull}) {
        throw QueryExecutionErrors.nullArgumentError("$nodeName", "expr");
      }
     """
    ev.copy(code = code, isNull = FalseLiteral, value = childGen.value)
  }

  override def inputTypes: Seq[AbstractDataType] = LongType :: Nil
}
