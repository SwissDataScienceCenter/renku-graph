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

package io.renku.knowledgegraph.metrics

import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.metrics._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

trait KGMetrics[F[_]] {
  def run(): F[Unit]
}

class KGMetricsImpl[F[_]: Temporal: Logger](
    statsFinder:    StatsFinder[F],
    countsGauge:    LabeledGauge[F, EntityLabel],
    initialDelay:   FiniteDuration = KGMetrics.initialDelay,
    countsInterval: FiniteDuration = KGMetrics.countsInterval
) extends KGMetrics[F] {

  def run(): F[Unit] = Temporal[F].delayBy(updateCounts().foreverM[Unit], initialDelay)

  private def updateCounts(): F[Unit] = {
    for {
      _      <- ().pure[F]
      counts <- statsFinder.entitiesCount()
      _      <- (counts map toCountsGauge).toList.sequence
      _      <- Temporal[F] sleep countsInterval
    } yield ()
  } recoverWith logAndRetry

  private lazy val toCountsGauge: ((EntityLabel, Count)) => F[Unit] = { case (status, count) =>
    countsGauge set status -> count.value.toDouble
  }

  private lazy val logAndRetry: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Temporal[F].andWait(Logger[F].error(exception)("Problem with gathering metrics"), initialDelay)
  }
}

object KGMetrics {

  import eu.timepit.refined.auto._

  import scala.concurrent.duration._

  private[metrics] val initialDelay:   FiniteDuration = 1 minute
  private[metrics] val countsInterval: FiniteDuration = 1 minute

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[KGMetrics[F]] = for {
    statsFinder <- StatsFinder[F]
    entitiesCountGauge <-
      Gauge[F, EntityLabel](
        name = "entities_count",
        help = "Total object by type.",
        labelName = "entities"
      )
  } yield new KGMetricsImpl[F](statsFinder, entitiesCountGauge)
}
