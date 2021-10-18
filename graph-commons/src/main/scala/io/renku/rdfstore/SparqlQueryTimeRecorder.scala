/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.rdfstore

import cats.MonadError
import cats.effect.{Clock, IO}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.Histogram
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import org.typelevel.log4cats.Logger

class SparqlQueryTimeRecorder[Interpretation[_]](val instance: ExecutionTimeRecorder[Interpretation])

object SparqlQueryTimeRecorder {

  private val QueryExecutionTimeLabel: String Refined NonEmpty = "query_id"
  private lazy val queriesExecutionTimesHistogram =
    Histogram
      .build()
      .name("sparql_execution_times")
      .labelNames(QueryExecutionTimeLabel.value)
      .help("Sparql execution times")
      .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 25, 50)

  import io.renku.metrics.MetricsRegistry

  def apply(
      metricsRegistry: MetricsRegistry[IO],
      logger:          Logger[IO] = ApplicationLogger
  )(implicit clock:    Clock[IO], ME: MonadError[IO, Throwable]): IO[SparqlQueryTimeRecorder[IO]] =
    for {
      histogram             <- metricsRegistry.register[Histogram, Histogram.Builder](queriesExecutionTimesHistogram)
      executionTimeRecorder <- ExecutionTimeRecorder[IO](logger, maybeHistogram = Some(histogram))
    } yield new SparqlQueryTimeRecorder(executionTimeRecorder)

}
