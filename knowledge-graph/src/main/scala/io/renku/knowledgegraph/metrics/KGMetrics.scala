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

package io.renku.knowledgegraph.metrics

import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import io.renku.logging.ApplicationLogger
import io.renku.metrics._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.util.control.NonFatal

trait KGMetrics[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class KGMetricsImpl[Interpretation[_]: ConcurrentEffect: Timer: Logger](
    statsFinder:    StatsFinder[Interpretation],
    countsGauge:    LabeledGauge[Interpretation, EntityLabel],
    initialDelay:   FiniteDuration = KGMetrics.initialDelay,
    countsInterval: FiniteDuration = KGMetrics.countsInterval
) extends KGMetrics[Interpretation] {

  def run(): Interpretation[Unit] = for {
    _ <- Timer[Interpretation] sleep initialDelay
    _ <- updateCounts().foreverM[Unit]
  } yield ()

  private def updateCounts(): Interpretation[Unit] = {
    for {
      _      <- ().pure[Interpretation]
      counts <- statsFinder.entitiesCount()
      _      <- (counts map toCountsGauge).toList.sequence
      _      <- Timer[Interpretation] sleep countsInterval
    } yield ()
  } recoverWith logAndRetry

  private lazy val toCountsGauge: ((EntityLabel, Count)) => Interpretation[Unit] = { case (status, count) =>
    countsGauge set status -> count.value.toDouble
  }

  private lazy val logAndRetry: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- Logger[Interpretation].error(exception)("Problem with gathering metrics")
      _ <- Timer[Interpretation] sleep initialDelay
    } yield ()
  }
}

object KGMetrics {

  import cats.effect.IO._
  import eu.timepit.refined.auto._

  import scala.concurrent.duration._

  private[metrics] val initialDelay:   FiniteDuration = 10 seconds
  private[metrics] val countsInterval: FiniteDuration = 1 minute

  def apply(
      metricsRegistry: MetricsRegistry[IO],
      timeRecorder:    SparqlQueryTimeRecorder[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext,
      logger:           Logger[IO]
  ): IO[KGMetrics[IO]] =
    for {
      statsFinder <- StatsFinder(timeRecorder, ApplicationLogger)
      entitiesCountGauge <-
        Gauge[IO, EntityLabel](
          name = "entities_count",
          help = "Total object by type.",
          labelName = "entities"
        )(metricsRegistry)
    } yield new KGMetricsImpl(statsFinder, entitiesCountGauge)
}
