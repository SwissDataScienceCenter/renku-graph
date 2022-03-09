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

import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.events.CategoryName
import io.renku.graph.model.events.EventStatus
import io.renku.metrics._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NonFatal

trait EventLogMetrics[F[_]] {
  def run(): F[Unit]
}

class EventLogMetricsImpl[F[_]: Temporal: Logger](
    statsFinder:             StatsFinder[F],
    categoryNameEventsGauge: LabeledGauge[F, CategoryName],
    statusesGauge:           LabeledGauge[F, EventStatus],
    totalGauge:              SingleValueGauge[F],
    interval:                FiniteDuration = EventLogMetrics.interval
) extends EventLogMetrics[F] {

  override def run(): F[Unit] = updateStatuses().foreverM[Unit]

  private def updateStatuses(): F[Unit] = for {
    _ <- Temporal[F] sleep interval
    _ <- provisionCategoryNames recoverWith logError(categoryNameEventsGauge.name)
    _ <- provisionStatuses recoverWith logError(statusesGauge.name)
  } yield ()

  private def provisionCategoryNames = for {
    eventsByCategoryName <- statsFinder.countEventsByCategoryName()
    _                    <- categoryNameEventsGauge.clear()
    _                    <- (eventsByCategoryName map toCategoryNameEventsGauge).toList.sequence
  } yield ()

  private lazy val toCategoryNameEventsGauge: ((CategoryName, Long)) => F[Unit] = { case (categoryName, count) =>
    categoryNameEventsGauge set (categoryName -> count.toDouble)
  }

  private def provisionStatuses = for {
    statuses <- statsFinder.statuses()
    _        <- (statuses map toStatusesGauge).toList.sequence
    _        <- totalGauge set statuses.values.sum.toDouble
  } yield ()

  private lazy val toStatusesGauge: ((EventStatus, Long)) => F[Unit] = { case (status, count) =>
    statusesGauge set (status -> count.toDouble)
  }

  private def logError(gaugeName: String): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Problem with gathering metrics for $gaugeName")
  }
}

object EventLogMetrics {

  private[metrics] val interval: FiniteDuration = 10 seconds

  import eu.timepit.refined.auto._

  def apply[F[_]: Temporal: Logger: MetricsRegistry](statsFinder: StatsFinder[F]): F[EventLogMetrics[F]] = for {
    categoryNameEventsGauge <- Gauge[F, CategoryName](
                                 name = "category_name_events_count",
                                 help = "Number of events waiting for processing per Category Name.",
                                 labelName = "category_name"
                               )
    statusesGauge <- Gauge[F, EventStatus](name = "events_statuses_count",
                                           help = "Total Commit Events by status.",
                                           labelName = "status"
                     )
    totalGauge <- Gauge(name = "events_count", help = "Total Commit Events.")
  } yield new EventLogMetricsImpl(statsFinder, categoryNameEventsGauge, statusesGauge, totalGauge)
}
