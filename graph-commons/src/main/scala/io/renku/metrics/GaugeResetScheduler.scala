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

package io.renku.metrics

import cats.MonadThrow
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.config.MetricsConfigProvider
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

trait GaugeResetScheduler[F[_]] {
  def run(): F[Unit]
}

class GaugeResetSchedulerImpl[F[_]: MonadThrow: Temporal: Logger, LabelValue](
    gauges:                 List[LabeledGauge[F, LabelValue]],
    metricsSchedulerConfig: MetricsConfigProvider[F]
) extends GaugeResetScheduler[F] {

  override def run(): F[Unit] = for {
    interval <- metricsSchedulerConfig.getInterval()
    _        <- resetGauges
    _        <- resetGaugesEvery(interval).foreverM[Unit]
  } yield ()

  private def resetGaugesEvery(interval: FiniteDuration): F[Unit] =
    ().pure[F] >> Temporal[F].delayBy(resetGauges, interval) recoverWith logError

  private def resetGauges: F[Unit] =
    gauges.map(_.reset() recoverWith logError).sequence.void

  private lazy val logError: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Clearing event gauge metrics failed")
  }
}

object GaugeResetScheduler {
  def apply[F[_]: MonadThrow: Temporal: Logger, LabelValue](
      gauges: List[LabeledGauge[F, LabelValue]],
      config: MetricsConfigProvider[F]
  ): F[GaugeResetScheduler[F]] = MonadThrow[F].catchNonFatal(
    new GaugeResetSchedulerImpl[F, LabelValue](gauges, config)
  )
}
