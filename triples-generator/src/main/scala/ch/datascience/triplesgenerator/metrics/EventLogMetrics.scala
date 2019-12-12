/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.metrics

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.commands.{EventLogStats, IOEventLogStats}
import ch.datascience.dbeventlog.{EventLogDB, EventStatus}
import ch.datascience.metrics.MetricsRegistry
import io.chrisdavenport.log4cats.Logger
import io.prometheus.client.Gauge

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class EventLogMetrics[Interpretation[_]](
    eventLogStats: EventLogStats[Interpretation],
    logger:        Logger[Interpretation],
    statusesGauge: Gauge = EventLogMetrics.statusesGauge,
    totalGauge:    Gauge = EventLogMetrics.totalGauge,
    interval:      FiniteDuration = EventLogMetrics.interval
)(implicit ME:     MonadError[Interpretation, Throwable], timer: Timer[Interpretation]) {

  def run: Interpretation[Unit] = (timer sleep interval) *> updateCollectors

  private def updateCollectors() = {
    for {
      statuses <- eventLogStats.statuses
      _ = statuses foreach setToGauge
      _ = totalGauge set statuses.values.sum
      _ <- run
    } yield ()
  } recoverWith logAndRetry

  private lazy val setToGauge: ((EventStatus, Long)) => Unit = {
    case (status, count) => statusesGauge.labels(status.toString).set(count)
  }

  private lazy val logAndRetry: PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Problem with gathering metrics")
      (timer sleep interval) *> run
  }
}

object EventLogMetrics {

  import scala.concurrent.duration._

  private val interval: FiniteDuration = 30 seconds

  private[metrics] val statusesGauge: Gauge = MetricsRegistry.register {
    Gauge
      .build()
      .name("events_statuses_count")
      .help("Total events by status.")
      .labelNames("status")
      .register(_)
  }

  private[metrics] val totalGauge: Gauge = MetricsRegistry.register {
    Gauge
      .build()
      .name("events_count")
      .help("Total events.")
      .register(_)
  }
}

object IOEventLogMetrics {

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO], timer: Timer[IO]): EventLogMetrics[IO] =
    new EventLogMetrics[IO](new IOEventLogStats(transactor), logger)
}
