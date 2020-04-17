/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.metrics

import cats.implicits._
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples.Sample

import scala.collection.JavaConverters._

object MetricsTools {

  implicit class CollectorOps(collector: Collector) {

    def collectAllSamples: Seq[(String, String, Double)] =
      for {
        familySamples <- collector.collect().asScala.toList
        sample        <- familySamples.samples.asScala.toList
        resultTuple   <- toResultTuple(sample)
      } yield resultTuple

    private val toResultTuple: Sample => List[(String, String, Double)] = sample =>
      (sample.labelNames.asScala.toList, sample.labelValues.asScala.toList) mapN {
        case (labelName, labelValue) => (labelName, labelValue, sample.value)
      }
  }
}
