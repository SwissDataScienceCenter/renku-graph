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

package ch.datascience.dbeventlog.metrics

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.{EventLogDB, EventStatus}
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.MetricsRegistry
import io.chrisdavenport.log4cats.Logger
import io.prometheus.client.Gauge

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class EventLogMetrics(
    statsFinder:           StatsFinder[IO],
    logger:                Logger[IO],
    waitingEventsGauge:    Gauge,
    statusesGauge:         Gauge,
    totalGauge:            Gauge,
    interval:              FiniteDuration = EventLogMetrics.interval,
    statusesInterval:      FiniteDuration = EventLogMetrics.statusesInterval,
    waitingEventsInterval: FiniteDuration = EventLogMetrics.waitingEventsInterval
)(implicit ME:             MonadError[IO, Throwable], timer: Timer[IO], cs: ContextShift[IO]) {

  def run: IO[Unit] = (timer sleep interval) *> updateCollectors

  private def updateCollectors() = {
    for {
      _ <- updateStatuses().start
      _ <- updateWaitingEvents().start
    } yield ()
  } recoverWith logAndRetry(continueWith = run)

  private def updateStatuses(): IO[Unit] = {
    for {
      statuses <- statsFinder.statuses
      _ = statuses foreach toStatusesGauge
      _ = totalGauge set statuses.values.sum
      _ <- (timer sleep statusesInterval) *> updateStatuses()
    } yield ()
  } recoverWith logAndRetry(continueWith = updateStatuses())

  private lazy val toStatusesGauge: ((EventStatus, Long)) => Unit = {
    case (status, count) => statusesGauge.labels(status.toString).set(count)
  }

  private def updateWaitingEvents(previousState: Map[Path, Long] = Map.empty): IO[Unit] = {
    for {
      waitingEvents <- statsFinder.waitingEvents
      newState = removeZeroCountProjects(waitingEvents, previousState)
      _        = newState foreach toWaitingEventsGauge
      _ <- (timer sleep waitingEventsInterval) *> updateWaitingEvents(newState)
    } yield ()
  } recoverWith logAndRetry(continueWith = updateWaitingEvents())

  private def removeZeroCountProjects(currentEvents: Map[Path, Long], previousState: Map[Path, Long]): Map[Path, Long] =
    if (previousState.isEmpty) currentEvents
    else {
      val currentZeros  = currentEvents.filter(_._2 == 0).keySet
      val previousZeros = previousState.filter(_._2 == 0).keySet
      val zerosToDelete = currentZeros intersect previousZeros
      zerosToDelete foreach (project => waitingEventsGauge remove project.toString)
      currentEvents.filterNot { case (project, _) => zerosToDelete contains project }
    }

  private lazy val toWaitingEventsGauge: ((Path, Long)) => Unit = {
    case (path, count) => waitingEventsGauge.labels(path.toString).set(count)
  }

  private def logAndRetry(continueWith: => IO[Unit]): PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Problem with gathering metrics")
      (timer sleep interval) *> continueWith
  }
}

object EventLogMetrics {

  import scala.concurrent.duration._

  private val interval:              FiniteDuration = 10 seconds
  private val statusesInterval:      FiniteDuration = 5 seconds
  private val waitingEventsInterval: FiniteDuration = 5 seconds

  private[metrics] val waitingEventsGaugeBuilder: Gauge.Builder =
    Gauge
      .build()
      .name("events_waiting_count")
      .help("Number of waiting Events by project path.")
      .labelNames("project")

  private[metrics] val statusesGaugeBuilder: Gauge.Builder =
    Gauge
      .build()
      .name("events_statuses_count")
      .help("Total Commit Events by status.")
      .labelNames("status")

  private[metrics] val totalGaugeBuilder: Gauge.Builder =
    Gauge
      .build()
      .name("events_count")
      .help("Total Commit Events.")
}

object IOEventLogMetrics {

  import EventLogMetrics._
  import cats.effect.IO._

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      logger:              Logger[IO],
      metricsRegistry:     MetricsRegistry[IO]
  )(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IO[EventLogMetrics] =
    for {
      waitingEventsGauge <- metricsRegistry.register[Gauge, Gauge.Builder](waitingEventsGaugeBuilder)
      statusesGauge      <- metricsRegistry.register[Gauge, Gauge.Builder](statusesGaugeBuilder)
      totalGauge         <- metricsRegistry.register[Gauge, Gauge.Builder](totalGaugeBuilder)
    } yield new EventLogMetrics(new IOStatsFinder(transactor), logger, waitingEventsGauge, statusesGauge, totalGauge)
}
