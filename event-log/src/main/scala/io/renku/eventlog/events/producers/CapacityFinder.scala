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

package io.renku.eventlog.events.producers

import cats.{Applicative, Id}
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.EventLogDB.SessionResource

private trait CapacityFinder[F[_]] {
  def findUsedCapacity: F[UsedCapacity]
}

private object CapacityFinder {

  def noOpCapacityFinder[F[_]: Applicative]: CapacityFinder[F] = new CapacityFinder[F] {
    override lazy val findUsedCapacity = UsedCapacity.zero.pure[F]
  }

  def queryBased[F[_]: Async: SessionResource: QueriesExecutionTimes](query: String): CapacityFinder[F] =
    new QueryBasedCapacityFinder[F](query)
}

private class QueryBasedCapacityFinder[F[_]: Async: SessionResource: QueriesExecutionTimes](query: String)
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with CapacityFinder[F] {

  import skunk._
  import skunk.codec.numeric._
  import skunk.implicits._

  override def findUsedCapacity: F[UsedCapacity] = SessionResource[F].useK(statement)

  private lazy val statement = measureExecutionTime {
    SqlStatement
      .named("find capacity")
      .select[Void, Long](sql"""#$query""".query(int8))
      .arguments(Void)
      .build[Id](_.unique)
      .mapResult(v => UsedCapacity(v.toInt))
  }
}
