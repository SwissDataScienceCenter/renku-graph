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
import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

trait EventGaugeScheduler[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class EventGaugeSchedulerImpl[Interpretation[_], LabelValue](
    gauges:                 List[LabeledGauge[Interpretation, LabelValue]],
    metricsSchedulerConfig: MetricsConfigProvider[Interpretation],
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends EventGaugeScheduler[Interpretation] {
  override def run(): Interpretation[Unit] = (for {
    interval <- metricsSchedulerConfig.getInterval()
    _        <- resetGaugesEvery(interval)
  } yield ()) recoverWith logError

  private def resetGaugesEvery(interval: FiniteDuration): Interpretation[Unit] = for {
    _ <- timer sleep interval
    _ <- gauges.map(_.reset()).sequence
    _ <- resetGaugesEvery(interval)
  } yield ()

  private lazy val logError: PartialFunction[Throwable, Interpretation[Unit]] = { case e: Throwable =>
    logger.error(s"Clearing event gauge metrics failed with - ${e.getMessage}")
  }
}

object IOEventGaugeScheduler {
  def apply[LabelValue](
      gauges:    List[LabeledGauge[IO, LabelValue]],
      logger:    Logger[IO]
  )(implicit ME: MonadError[IO, Throwable], timer: Timer[IO]): IO[EventGaugeSchedulerImpl[IO, LabelValue]] = IO(
    new EventGaugeSchedulerImpl[IO, LabelValue](gauges, new MetricsConfigProviderImpl[IO](), logger)
  )
}
