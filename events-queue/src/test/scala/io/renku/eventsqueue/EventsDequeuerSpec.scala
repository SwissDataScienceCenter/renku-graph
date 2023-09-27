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

import Generators.events
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref, Temporal}
import fs2.Stream
import io.circe.syntax._
import io.renku.db.syntax._
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import scala.concurrent.duration._

class EventsDequeuerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventsQueueDBSpec
    with should.Matchers
    with AsyncMockFactory {

  it should "register the given handler that gets fed with a stream of events found for the category " +
    "even before any notification is sent" in {

      val category = categoryNames.generateOne

      val handledEvents = Ref.unsafe[IO, List[String]](Nil)
      val handler: Stream[IO, String] => IO[Unit] =
        _.evalTap(e => handledEvents.update(l => (e :: l.reverse).reverse)).compile.drain

      val dequeuedEvents       = events.generateList(min = 1).map(_.asJson.noSpaces)
      val dequeuedEventsStream = Stream.emits[IO, String](dequeuedEvents)
      givenFindingChunkOfEvents(category, returning = QueryDef.pure(dequeuedEventsStream))

      dequeuer.registerHandler(category, handler).assertNoException >>
        handledEvents.get.asserting(_ shouldBe dequeuedEvents)
    }

  it should "register the given handler that gets woken up on a notification is sent or the category channel" in {

    val category = categoryNames.generateOne

    val handledEvents = Ref.unsafe[IO, List[String]](Nil)
    val handler: Stream[IO, String] => IO[Unit] =
      _.evalTap(e => handledEvents.update(l => (e :: l.reverse).reverse)).compile.drain

    val initialEvents       = events.generateList(min = 1).map(_.asJson.noSpaces)
    val initialEventsStream = Stream.emits[IO, String](initialEvents)
    givenFindingChunkOfEvents(category, returning = QueryDef.pure(initialEventsStream))

    val dequeuedEvents       = events.generateList(min = 1).map(_.asJson.noSpaces)
    val dequeuedEventsStream = Stream.emits[IO, String](dequeuedEvents)
    givenFindingChunkOfEvents(category, returning = QueryDef.pure(dequeuedEventsStream))

    dequeuer.registerHandler(category, handler).assertNoException >>
      handledEvents.get.asserting(_ shouldBe initialEvents) >>
      Temporal[IO].sleep(1 second) >>
      notify(category.asChannelId, dequeuedEvents.head.asJson) >>
      Temporal[IO].sleep(2 seconds) >>
      handledEvents.get.asserting(_ shouldBe initialEvents ::: dequeuedEvents)
  }

  private val dbRepository  = mock[DBRepository[IO]]
  private lazy val dequeuer = new EventsDequeuerImpl[IO, TestDB](dbRepository)

  private def givenFindingChunkOfEvents(category: CategoryName, returning: QueryDef[IO, Stream[IO, String]]) =
    (dbRepository.fetchChunk _)
      .expects(category)
      .returning(returning)
}
