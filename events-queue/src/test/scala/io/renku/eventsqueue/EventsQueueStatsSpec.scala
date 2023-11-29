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

import cats.effect.IO
import cats.syntax.all._
import io.circe.syntax._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.syntax.QueryDef
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.eventsqueue.Generators.events
import io.renku.eventsqueue.TypeSerializers.categoryNameEncoder
import io.renku.generators.Generators.Implicits._
import io.renku.testtools.CustomAsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.codec.all.{int4, text}
import skunk.implicits._

class EventsQueueStatsSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with EventsQueuePostgresSpec
    with should.Matchers {

  it should "return stats about number of events per category" in testDBResource.use { implicit cfg =>
    val category1       = categoryNames.generateOne
    val category1Events = events.generateList(min = 1).map(_.asJson)
    val category2       = categoryNames.generateOne
    val category2Events = events.generateList(min = 1).map(_.asJson)

    val stats = new EventsQueueStatsImpl[IO, TestDB]

    for {
      _ <- category1Events.traverse_(e => execute(repo.insert(category1, e)))
      _ <- category2Events.traverse_(e => execute(repo.insert(category2, e)))
      res <- execute(stats.countsByCategory).asserting(
               _ shouldBe Map(category1 -> category1Events.size.toLong, category2 -> category2Events.size.toLong)
             )
    } yield res
  }

  it should "return zero value if there are no events for the category anymore" in testDBResource.use { implicit cfg =>
    val category1       = categoryNames.generateOne
    val category1Events = events.generateList(min = 1).map(_.asJson)
    val category2       = categoryNames.generateOne
    val category2Events = events.generateList(min = 1).map(_.asJson)

    val stats = new EventsQueueStatsImpl[IO, TestDB]

    for {
      _ <- category1Events.traverse_(e => execute(repo.insert(category1, e)))
      _ <- category2Events.traverse_(e => execute(repo.insert(category2, e)))
      _ <- execute(stats.countsByCategory).asserting(
             _ shouldBe Map(category1 -> category1Events.size.toLong, category2 -> category2Events.size.toLong)
           )
      category1DBEvents <- findEvents(category1)
      _ <- category1DBEvents.traverse_(e => execute(repo.markUnderProcessing(e.id)) >> execute(repo.delete(e.id)))
      res <- execute(stats.countsByCategory).asserting(
               _ shouldBe Map(category1 -> 0L, category2 -> category2Events.size.toLong)
             )
    } yield res
  }

  private lazy val repo = new EventsRepositoryImpl[IO, TestDB]

  private def findEvents(category: CategoryName)(implicit cfg: DBConfig[TestDB]): IO[List[DequeuedEvent]] = {
    val query: Query[CategoryName, DequeuedEvent] =
      sql"""SELECT id, payload
            FROM enqueued_event
            WHERE category = $categoryNameEncoder"""
        .query(int4 ~ text)
        .map { case (id: Int, payload: String) => DequeuedEvent(id, payload) }

    execute {
      QueryDef[IO, List[DequeuedEvent]] {
        _.prepare(query).flatMap(_.stream(category, 10).compile.toList)
      }
    }
  }
}
