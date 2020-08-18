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

package ch.datascience.knowledgegraph.metrics

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.metrics._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class KGEntitiesMetrics(
    statsFinder:    StatsFinder[IO],
    logger:         Logger[IO],
    countsGauge:    LabeledGauge[IO, KGEntityType],
    interval:       FiniteDuration = KGEntitiesMetrics.interval,
    countsInterval: FiniteDuration = KGEntitiesMetrics.statusesInterval
)(implicit ME:      MonadError[IO, Throwable], timer: Timer[IO], cs: ContextShift[IO]) {

  def run: IO[Unit] =
    for {
      _ <- timer sleep interval
      _ <- updateCounts()
    } yield ()

  private def updateCounts(): IO[Unit] = {
    for {
      counts <- statsFinder.entitiesCount
      _      <- (counts map toCountsGauge).toList.sequence
      _      <- timer sleep countsInterval
      _      <- updateCounts()
    } yield ()
  } recoverWith logAndRetry(continueWith = updateCounts())

  private lazy val toCountsGauge: ((KGEntityType, Long)) => IO[Unit] = {
    case (status, count) => countsGauge set status -> count
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

object KGEntitiesMetrics {

  import scala.concurrent.duration._

  private val interval:         FiniteDuration = 10 seconds
  private val statusesInterval: FiniteDuration = 5 seconds
}

object IOKGEntitiesMetrics {

  import cats.effect.IO._
  import eu.timepit.refined.auto._

  def apply(
      statsFinder:         StatsFinder[IO],
      logger:              Logger[IO],
      metricsRegistry:     MetricsRegistry[IO]
  )(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IO[KGEntitiesMetrics] =
    for {
      statusesGauge <- Gauge[IO, KGEntityType](name = "entities_count",
                                               help      = "Total object by type.",
                                               labelName = "entities")(metricsRegistry)

    } yield new KGEntitiesMetrics(statsFinder, logger, statusesGauge)
}
