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

package ch.datascience.metrics

import cats.MonadError
import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.config.MetricsConfigProvider
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

trait GaugeResetScheduler[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class GaugeResetSchedulerImpl[Interpretation[_], LabelValue](
    gauges:                 List[LabeledGauge[Interpretation, LabelValue]],
    metricsSchedulerConfig: MetricsConfigProvider[Interpretation],
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends GaugeResetScheduler[Interpretation] {

  override def run(): Interpretation[Unit] = for {
    interval <- metricsSchedulerConfig.getInterval()
    _        <- resetGauges
    _        <- resetGaugesEvery(interval).foreverM[Unit]
  } yield ()

  private def resetGaugesEvery(interval: FiniteDuration): Interpretation[Unit] = {
    for {
      _ <- timer sleep interval
      _ <- resetGauges
    } yield ()
  } recoverWith logError

  private def resetGauges: Interpretation[Unit] =
    gauges.map(_.reset() recoverWith logError).sequence.void

  private lazy val logError: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"Clearing event gauge metrics failed")
  }
}

object IOGaugeResetScheduler {
  def apply[LabelValue](
      gauges:    List[LabeledGauge[IO, LabelValue]],
      config:    MetricsConfigProvider[IO],
      logger:    Logger[IO]
  )(implicit ME: MonadError[IO, Throwable], timer: Timer[IO]): IO[GaugeResetScheduler[IO]] = IO(
    new GaugeResetSchedulerImpl[IO, LabelValue](gauges, config, logger)
  )
}
