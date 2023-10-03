/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.metrics

import cats.Traverse
import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import fs2.{Compiler, Stream}
import io.renku.lock.PostgresLockStats
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.TgDB
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

trait MetricsService[F[_]] {

  def collect: F[Unit]

  def collectEvery(interval: FiniteDuration)(implicit C: Compiler[F, F], T: Temporal[F], L: Logger[F]): F[Unit] =
    Stream
      .awakeEvery(interval)
      .evalMap(_ => collect.handleErrorWith(L.error(_)("An error during Metrics collection")))
      .compile
      .drain
}

object MetricsService {

  def apply[F[_]: Async: MetricsRegistry](dbPool: TgDB.SessionResource[F]): F[MetricsService[F]] = {
    val pgGauge  = PostgresLockGauge[F]("triples_generator")
    val pgHg     = PostgresLockHistogram[F]
    val getStats = dbPool.useK(Kleisli(PostgresLockStats.getStats[F]))

    (pgGauge, pgHg).mapN { (gauge, hg) =>
      new MetricsService[F] {
        override def collect: F[Unit] =
          getStats.flatMap { stats =>
            Traverse[List].sequence_(
              gauge.set(PostgresLockGauge.Label.CurrentLocks -> stats.currentLocks.toDouble) ::
                gauge.set(PostgresLockGauge.Label.Waiting -> stats.waiting.size.toDouble) ::
                stats.waiting.map(w => hg.observe(w.objectId.toString, w.waitDuration.toMillis.toDouble))
            )
          }
      }
    }
  }
}
