/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.metrics

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.metrics.{LabeledHistogram, LabeledHistogramImpl, MetricsRegistry}

import scala.concurrent.duration._

trait QueriesExecutionTimes[F[_]] extends LabeledHistogram[F]

object QueriesExecutionTimes {

  def apply[F[_]: MonadThrow: MetricsRegistry](): F[QueriesExecutionTimes[F]] = MetricsRegistry[F].register {
    new LabeledHistogramImpl[F](
      name = "event_log_queries_execution_times",
      help = "Event Log queries execution times",
      labelName = "query_id",
      buckets = Seq(.05, .1, .5, 1, 2.5, 5, 10, 50),
      maybeThreshold = (750 millis).some
    ) with QueriesExecutionTimes[F]
  }.widen

  def apply[F[_]](implicit ev: QueriesExecutionTimes[F]): QueriesExecutionTimes[F] = ev
}
