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

package org.apache.spark.kafka010

import org.apache.kafka.common.config.SaslConfigs

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.util.Utils.{redact, REDACTION_REPLACEMENT_TEXT}

object KafkaRedactionUtil extends Logging {
  def redactParams(params: Seq[(String, Object)]): Seq[(String, String)] = {
    val redactionPattern = Some(Option(SparkEnv.get).map(_.conf)
      .getOrElse(new SparkConf()).get(SECRET_REDACTION_PATTERN))
    params.map { case (key, value) =>
      if (value != null) {
        if (key.equalsIgnoreCase(SaslConfigs.SASL_JAAS_CONFIG)) {
          (key, redactJaasParam(value.asInstanceOf[String]))
        } else {
          val (_, newValue) = redact(redactionPattern, Seq((key, value.toString))).head
          (key, newValue)
        }
      } else {
        (key, value.asInstanceOf[String])
      }
    }
  }

  // Matches JAAS credential entries such as `password="..."`, `password='...'`, `password=value`
  // and `clientSecret=...` (used by the OAUTHBEARER login module), regardless of quoting style.
  // The previous implementation only matched double-quoted `password="..."`, so single-quoted,
  // unquoted, and non-password credential fields (e.g. OAUTHBEARER clientSecret) were not redacted.
  private val jaasCredentialPattern =
    "(?i)(password|clientSecret)\\s*=\\s*(\"[^\"]*\"|'[^']*'|[^\\s;]+)".r

  def redactJaasParam(param: String): String = {
    if (param != null && !param.isEmpty) {
      jaasCredentialPattern.replaceAllIn(
        param, m => s"""${m.group(1)}="$REDACTION_REPLACEMENT_TEXT"""")
    } else {
      param
    }
  }
}
