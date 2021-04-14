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

package io.renku.eventlog.metrics

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.events.{CategoryName, EventStatus}
import ch.datascience.metrics._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

trait EventLogMetrics[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class EventLogMetricsImpl(
    statsFinder:             StatsFinder[IO],
    logger:                  Logger[IO],
    categoryNameEventsGauge: LabeledGauge[IO, CategoryName],
    statusesGauge:           LabeledGauge[IO, EventStatus],
    totalGauge:              SingleValueGauge[IO],
    interval:                FiniteDuration = EventLogMetrics.interval
)(implicit ME:               MonadError[IO, Throwable], timer: Timer[IO], cs: ContextShift[IO])
    extends EventLogMetrics[IO] {

  override def run(): IO[Unit] = updateStatuses().foreverM[Unit]

  private def updateStatuses(): IO[Unit] = for {
    _ <- timer sleep interval
    _ <- provisionCategoryNames recoverWith logError(categoryNameEventsGauge.name)
    _ <- provisionStatuses recoverWith logError(statusesGauge.name)
  } yield ()

  private def provisionCategoryNames = for {
    eventsByCategoryName <- statsFinder.countEventsByCategoryName()
    _                    <- (eventsByCategoryName map toCategoryNameEventsGauge).toList.sequence
  } yield ()

  private lazy val toCategoryNameEventsGauge: ((CategoryName, Long)) => IO[Unit] = { case (categoryName, count) =>
    categoryNameEventsGauge set (categoryName -> count.toDouble)
  }

  private def provisionStatuses = for {
    statuses <- statsFinder.statuses()
    _        <- (statuses map toStatusesGauge).toList.sequence
    _        <- totalGauge set statuses.values.sum.toDouble
  } yield ()

  private lazy val toStatusesGauge: ((EventStatus, Long)) => IO[Unit] = { case (status, count) =>
    statusesGauge set (status -> count.toDouble)
  }

  private def logError(gaugeName: String): PartialFunction[Throwable, IO[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"Problem with gathering metrics for $gaugeName")
  }
}

object EventLogMetrics {
  private[metrics] val interval: FiniteDuration = 10 seconds
}

object IOEventLogMetrics {

  import cats.effect.IO._
  import eu.timepit.refined.auto._

  def apply(
      statsFinder:         StatsFinder[IO],
      logger:              Logger[IO],
      metricsRegistry:     MetricsRegistry[IO]
  )(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IO[EventLogMetrics[IO]] =
    for {
      categoryNameEventsGauge <- Gauge[IO, CategoryName](
                                   name = "category_name_events_count",
                                   help = "Number of events waiting for processing per Category Name.",
                                   labelName = "category_name"
                                 )(metricsRegistry)
      statusesGauge <- Gauge[IO, EventStatus](name = "events_statuses_count",
                                              help = "Total Commit Events by status.",
                                              labelName = "status"
                       )(metricsRegistry)
      totalGauge <- Gauge(
                      name = "events_count",
                      help = "Total Commit Events."
                    )(metricsRegistry)
    } yield new EventLogMetricsImpl(statsFinder, logger, categoryNameEventsGauge, statusesGauge, totalGauge)
}
