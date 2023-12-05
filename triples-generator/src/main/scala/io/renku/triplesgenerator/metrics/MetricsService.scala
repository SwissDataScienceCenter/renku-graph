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
import io.renku.eventsqueue.EventsQueueStats
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

  def apply[F[_]: Async: MetricsRegistry](dbPool: TgDB.SessionResource[F]): F[MetricsService[F]] =
    for {
      locksGauge          <- PostgresLockGauge[F]("triples_generator")
      locksHg             <- PostgresLockHistogram[F]
      categoryEventsCount <- CategoryEventsCountGauge[F]
    } yield new MetricsServiceImpl[F](locksGauge, locksHg, categoryEventsCount)(dbPool)
}

private class MetricsServiceImpl[F[_]: Async](lockGauge: PostgresLockGauge[F],
                                              lockHistogram:            PostgresLockHistogram[F],
                                              categoryEventsCountGauge: CategoryEventsCountGauge[F]
)(dbPool: TgDB.SessionResource[F])
    extends MetricsService[F] {

  override def collect: F[Unit] =
    updateLocksStats() >> updateCategoryEventCounts()

  private val getPostgresLocksStats = dbPool.useK(Kleisli(PostgresLockStats.getStats[F](dbPool.dbName, _)))

  private def updateLocksStats() =
    getPostgresLocksStats >>= { stats =>
      Traverse[List].sequence_(
        lockGauge.set(PostgresLockGauge.Label.CurrentLocks -> stats.currentLocks.toDouble) ::
          lockGauge.set(PostgresLockGauge.Label.Waiting -> stats.waiting.size.toDouble) ::
          stats.waiting.map(w => lockHistogram.observe(w.objectId.toString, w.waitDuration.toMillis.toDouble))
      )
    }

  private val getCategoryCountsStats = dbPool.useK(EventsQueueStats[F, TgDB].countsByCategory)

  private def updateCategoryEventCounts() =
    getCategoryCountsStats >>= {
      _.toList.traverse_ { case (category, count) => categoryEventsCountGauge.set(category -> count.toDouble) }
    }
}
