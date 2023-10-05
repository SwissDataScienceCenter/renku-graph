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

package io.renku.eventsqueue

import DBInfra.QueueTable
import DBInfra.QueueTable.Column
import cats.effect.Async
import io.renku.db.syntax._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.events.CategoryName
import io.renku.eventsqueue.TypeSerializers.categoryNameDecoder
import skunk.Void
import skunk.codec.all.int8
import skunk.implicits._

trait EventsQueueStats[F[_]] {
  def countsByCategory: QueryDef[F, Map[CategoryName, Long]]
}

object EventsQueueStats {
  def apply[F[_]: Async, DB]: EventsQueueStats[F] =
    new EventsQueueStatsImpl[F, DB]
}

private class EventsQueueStatsImpl[F[_]: Async, DB]
    extends DbClient[F](maybeHistogram = None)
    with EventsQueueStats[F] {

  override def countsByCategory: QueryDef[F, Map[CategoryName, Long]] = measureExecutionTime {
    SqlStatement
      .named(s"$queryPrefix stats")
      .select[Void, (CategoryName, Long)](
        sql"""SELECT #${Column.category}, COUNT(#${Column.id}) AS count
              FROM #${QueueTable.name}
              GROUP BY #${Column.category}"""
          .query(categoryNameDecoder ~ int8)
      )
      .arguments(Void)
      .build(_.toList)
      .mapResult(_.toMap)
  }

  private lazy val queryPrefix = "queue event -"
}
