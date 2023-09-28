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

import Generators.dequeuedEvents
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref, Temporal}
import cats.syntax.all._
import fs2.{Pipe, Stream}
import io.renku.db.syntax._
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
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
    "even before any notification is received" in {

      val category = categoryNames.generateOne

      val initialEvents = dequeuedEvents.generateList(min = 1)
      givenFindingEvents(category, returning = QueryDef.pure(Stream.emits[IO, DequeuedEvent](initialEvents)))

      givenUpdatingEvents(initialEvents, returning = CommandDef.pure[IO])
      givenRemovingEvents(initialEvents, returning = CommandDef.pure[IO])

      val handler = new AccumulatingHandler

      dequeuer.registerHandler(category, handler).assertNoException >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents)
    }

  it should "fed the given handler with a stream of events " +
    "after the category event is received" in {

      val category = categoryNames.generateOne

      val initialEvents = dequeuedEvents.generateList(min = 1)
      givenFindingEvents(category, returning = QueryDef.pure(Stream.emits[IO, DequeuedEvent](initialEvents)))

      val onNotifEvents = dequeuedEvents.generateList(min = 1)
      givenFindingEvents(category, returning = QueryDef.pure(Stream.emits[IO, DequeuedEvent](onNotifEvents)))

      val allEvents = initialEvents ::: onNotifEvents
      givenUpdatingEvents(allEvents, returning = CommandDef.pure[IO])
      givenRemovingEvents(allEvents, returning = CommandDef.pure[IO])

      val handler = new AccumulatingHandler

      dequeuer.registerHandler(category, handler).assertNoException >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents) >>
        Temporal[IO].sleep(1 second) >>
        notify(category.asChannelId, onNotifEvents.head.payload) >>
        Temporal[IO].sleep(2 seconds) >>
        handler.handledEvents.get.asserting(_ shouldBe allEvents)
    }

  it should "handle the error occurring during category event processing " +
    "and prevent the process from dying" in {

      val category = categoryNames.generateOne

      val initialEvents = dequeuedEvents.generateList(min = 1)
      givenFindingEvents(category, returning = QueryDef.pure(Stream.emits[IO, DequeuedEvent](initialEvents)))

      val beforeFailingEvent = dequeuedEvents.generateOne
      val failingEvent       = dequeuedEvents.generateOne
      val afterFailingEvent  = dequeuedEvents.generateOne
      givenFindingEvents(
        category,
        returning = QueryDef.pure(Stream[IO, DequeuedEvent](beforeFailingEvent, failingEvent, afterFailingEvent))
      )

      val onNotifEvents = dequeuedEvents.generateList(min = 1)
      givenFindingEvents(category, returning = QueryDef.pure(Stream.emits[IO, DequeuedEvent](onNotifEvents)))

      givenUpdatingEvents(initialEvents ::: beforeFailingEvent :: failingEvent :: onNotifEvents,
                          returning = CommandDef.pure[IO]
      )
      givenRemovingEvents(initialEvents ::: beforeFailingEvent :: onNotifEvents, returning = CommandDef.pure[IO])

      val handler = new AccumulatingHandler(maybeFailOnEvent = failingEvent.some)

      dequeuer.registerHandler(category, handler).assertNoException >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents) >>
        Temporal[IO].sleep(1 second) >>
        notify(category.asChannelId, failingEvent.payload) >>
        Temporal[IO].sleep(2 seconds) >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents ::: beforeFailingEvent :: Nil) >>
        notify(category.asChannelId, onNotifEvents.head.payload) >>
        Temporal[IO].sleep(2 seconds) >>
        handler.handledEvents.get.asserting(_ shouldBe (initialEvents ::: beforeFailingEvent :: onNotifEvents))
    }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private val dbRepository  = mock[DBRepository[IO]]
  private lazy val dequeuer = new EventsDequeuerImpl[IO, TestDB](dbRepository)

  private def givenFindingEvents(category: CategoryName, returning: QueryDef[IO, Stream[IO, DequeuedEvent]]) =
    (dbRepository.eventsStream _)
      .expects(category)
      .returning(returning)

  private def givenUpdatingEvents(events: List[DequeuedEvent], returning: CommandDef[IO]) =
    events.map(givenUpdatingEvent(_, returning))

  private def givenUpdatingEvent(event: DequeuedEvent, returning: CommandDef[IO]) =
    (dbRepository.markUnderProcessing _)
      .expects(event.id)
      .returning(returning)

  private def givenRemovingEvents(events: List[DequeuedEvent], returning: CommandDef[IO]) =
    events.map(givenRemovingEvent(_, returning))

  private def givenRemovingEvent(event: DequeuedEvent, returning: CommandDef[IO]) =
    (dbRepository.delete _)
      .expects(event.id)
      .returning(returning)

  private class AccumulatingHandler(maybeFailOnEvent: Option[DequeuedEvent] = None)
      extends Pipe[IO, DequeuedEvent, DequeuedEvent] {

    val handledEvents = Ref.unsafe[IO, List[DequeuedEvent]](Nil)

    override def apply(stream: Stream[IO, DequeuedEvent]): Stream[IO, DequeuedEvent] =
      stream.evalTap {
        case e if maybeFailOnEvent contains e => exceptions.generateOne.raiseError[IO, Unit]
        case e                                 => handledEvents.update(_ appended e)
      }
  }
}
