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
import io.circe.Json
import io.renku.db.syntax._
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.lock.Lock
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

      val initialEvents = dequeuedEvents.generateList(min = 1)
      val repository    = createRepository(initialEvents)

      val handler = new AccumulatingHandler

      dequeuer(repository).registerHandler(category, handler).assertNoException >>
        repository.processed.get.asserting(_ shouldBe initialEvents.map(_.id)) >>
        repository.deleted.get.asserting(_ shouldBe initialEvents.map(_.id)) >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents)
    }

  it should "fed the given handler with a stream of events " +
    "after the category event is received" in {

      val initialEvents = dequeuedEvents.generateList(min = 1)
      val onNotifEvents = dequeuedEvents.generateList(min = 1)
      val allEvents     = initialEvents ::: onNotifEvents
      val repository    = createRepository(initialEvents, onNotifEvents)

      val handler = new AccumulatingHandler

      dequeuer(repository).registerHandler(category, handler).assertNoException >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents) >>
        Temporal[IO].sleep(1 second) >>
        notify(category.asChannelId, onNotifEvents.head.payload) >>
        Temporal[IO].sleep(2 seconds) >>
        handler.handledEvents.get.asserting(_ shouldBe allEvents) >>
        repository.processed.get.asserting(_ shouldBe allEvents.map(_.id)) >>
        repository.deleted.get.asserting(_ shouldBe allEvents.map(_.id))
    }

  it should "handle the error occurring during category event processing " +
    "and prevent the process from dying" in {

      // batch 1
      val initialEvents = dequeuedEvents.generateList(min = 1)

      // batch 2
      val beforeFailingEvent = dequeuedEvents.generateOne
      val failingEvent       = dequeuedEvents.generateOne
      val afterFailingEvent  = dequeuedEvents.generateOne

      // batch 3
      val onNotifEvents = dequeuedEvents.generateList(min = 1)

      val repository =
        createRepository(initialEvents, List(beforeFailingEvent, failingEvent, afterFailingEvent), onNotifEvents)

      val handler = new AccumulatingHandler(maybeFailOnEvent = failingEvent.some)

      dequeuer(repository).registerHandler(category, handler).assertNoException >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents) >>
        Temporal[IO].sleep(1 second) >>
        notify(category.asChannelId, failingEvent.payload) >>
        Temporal[IO].sleep(2 seconds) >>
        handler.handledEvents.get.asserting(_ shouldBe initialEvents ::: beforeFailingEvent :: Nil) >>
        notify(category.asChannelId, onNotifEvents.head.payload) >>
        Temporal[IO].sleep(2 seconds) >>
        handler.handledEvents.get.asserting(_ shouldBe (initialEvents ::: beforeFailingEvent :: onNotifEvents)) >>
        repository.processed.get.asserting(
          _ shouldBe (initialEvents ::: beforeFailingEvent :: failingEvent :: onNotifEvents).map(_.id)
        ) >>
        repository.deleted.get.asserting(_ shouldBe (initialEvents ::: beforeFailingEvent :: onNotifEvents).map(_.id))
    }

  private lazy val category = categoryNames.generateOne

  private val tsWriteLock: Lock[IO, CategoryName] = Lock.none[IO, CategoryName]
  private def dequeuer(repository: Repository) = new EventsDequeuerImpl[IO, TestDB](repository, tsWriteLock)

  private def createRepository(eventBatches: List[DequeuedEvent]*) =
    new Repository(
      eventBatches.toList.map(Stream.emits[IO, DequeuedEvent])
    )

  private class Repository(eventBatches: List[Stream[IO, DequeuedEvent]]) extends DBRepository[IO] {

    override def insert(category: CategoryName, payload: Json): CommandDef[IO] = fail("Shouldn't be called")

    private val batchesIterator = eventBatches.iterator

    override def processEvents(category:       CategoryName,
                               transformation: Pipe[IO, DequeuedEvent, Unit]
    ): QueryDef[IO, Unit] =
      QueryDef.liftF(
        batchesIterator
          .nextOption()
          .getOrElse(Stream.empty)
          .through(transformation)
          .compile
          .drain
      )

    val processed = Ref.unsafe[IO, List[Int]](Nil)

    override def markUnderProcessing(eventId: Int): CommandDef[IO] =
      CommandDef.liftF(processed.update(_ appended eventId))

    val deleted = Ref.unsafe[IO, List[Int]](Nil)

    override def delete(eventId: Int): CommandDef[IO] =
      CommandDef.liftF(deleted.update(_ appended eventId))
  }

  private class AccumulatingHandler(maybeFailOnEvent: Option[DequeuedEvent] = None)
      extends Pipe[IO, DequeuedEvent, DequeuedEvent] {

    val handledEvents = Ref.unsafe[IO, List[DequeuedEvent]](Nil)

    override def apply(stream: Stream[IO, DequeuedEvent]): Stream[IO, DequeuedEvent] =
      stream.evalTap {
        case e if maybeFailOnEvent contains e => exceptions.generateOne.raiseError[IO, Unit]
        case e                                => handledEvents.update(_ appended e)
      }
  }
}
