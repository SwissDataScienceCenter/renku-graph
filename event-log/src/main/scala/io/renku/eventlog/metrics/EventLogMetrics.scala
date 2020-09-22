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

package io.renku.eventlog.metrics

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.metrics._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventStatus

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class EventLogMetrics(
    statsFinder:      StatsFinder[IO],
    logger:           Logger[IO],
    statusesGauge:    LabeledGauge[IO, EventStatus],
    totalGauge:       SingleValueGauge[IO],
    interval:         FiniteDuration = EventLogMetrics.interval,
    statusesInterval: FiniteDuration = EventLogMetrics.statusesInterval
)(implicit ME:        MonadError[IO, Throwable], timer: Timer[IO], cs: ContextShift[IO]) {

  def run: IO[Unit] =
    for {
      _ <- timer sleep interval
      _ <- updateStatuses()
    } yield ()

  private def updateStatuses(): IO[Unit] = {
    for {
      statuses <- statsFinder.statuses
      _        <- (statuses map toStatusesGauge).toList.sequence
      _        <- totalGauge set statuses.values.sum
      _        <- timer sleep statusesInterval
      _        <- updateStatuses()
    } yield ()
  } recoverWith logAndRetry(continueWith = updateStatuses())

  private lazy val toStatusesGauge: ((EventStatus, Long)) => IO[Unit] = { case (status, count) =>
    statusesGauge set status -> count
  }

  private def logAndRetry(continueWith: => IO[Unit]): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)("Problem with gathering metrics")
        _ <- timer sleep interval
        _ <- continueWith
      } yield ()
  }
}

object EventLogMetrics {

  import scala.concurrent.duration._

  private val interval:         FiniteDuration = 10 seconds
  private val statusesInterval: FiniteDuration = 5 seconds
}

object IOEventLogMetrics {

  import cats.effect.IO._
  import eu.timepit.refined.auto._

  def apply(
      statsFinder:         StatsFinder[IO],
      logger:              Logger[IO],
      metricsRegistry:     MetricsRegistry[IO]
  )(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IO[EventLogMetrics] =
    for {
      statusesGauge <- Gauge[IO, EventStatus](name = "events_statuses_count",
                                              help = "Total Commit Events by status.",
                                              labelName = "status"
                       )(metricsRegistry)
      totalGauge <- Gauge(
                      name = "events_count",
                      help = "Total Commit Events."
                    )(metricsRegistry)
    } yield new EventLogMetrics(statsFinder, logger, statusesGauge, totalGauge)
}
