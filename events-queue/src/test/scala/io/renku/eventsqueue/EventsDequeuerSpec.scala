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
import cats.effect.{IO, Ref}
import fs2.concurrent.SignallingRef
import fs2.{Chunk, Pipe, Stream}
import io.circe.Json
import io.renku.db.syntax._
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.lock.Lock
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventsDequeuerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventsQueueDBSpec
    with should.Matchers
    with AsyncMockFactory {

  "acquireEventsStream" should {

    "obtain an infinite Stream of chunks of events from the repository " +
      "which status is already updated before passing to the handler " +
      "and deleted afterwards " +
      "- case when there are chunks of events in the queue before any notification is received" in {

        val initialEvents1 = dequeuedEvents.generateList(min = 1)
        val initialEvents2 = dequeuedEvents.generateList(min = 1)
        val repository     = createRepository(initialEvents1, initialEvents2)

        val handler = new AccumulatingHandler

        val allEvents = initialEvents1 ::: initialEvents2

        for {
          deqFiber <- dequeuer(repository).acquireEventsStream(category).through(handler).compile.drain.start
          _        <- handler.handledEvents.waitUntil(_ == allEvents)
          _        <- deqFiber.cancel
          _        <- repository.processed.get.asserting(_ shouldBe allEvents.map(_.id))
          res      <- repository.deleted.get.asserting(_ shouldBe allEvents.map(_.id))
        } yield res
      }

    "continue receiving subsequent chunks of events on every notification received from the channel" in {

      val initialEvents = dequeuedEvents.generateList(min = 1)
      val repository    = createRepository(initialEvents)

      val handler = new AccumulatingHandler

      val onNotifEvents = dequeuedEvents.generateList(min = 1)
      val allEvents     = initialEvents ::: onNotifEvents

      for {
        deqFiber <- dequeuer(repository).acquireEventsStream(category).through(handler).compile.drain.start
        _        <- handler.handledEvents.waitUntil(_ == initialEvents)
        _        <- repository.addEventBatch(onNotifEvents)
        _        <- notify(category)
        _        <- handler.handledEvents.waitUntil(_ == allEvents)
        _        <- deqFiber.cancel
        _        <- repository.processed.get.asserting(_ shouldBe allEvents.map(_.id))
        res      <- repository.deleted.get.asserting(_ shouldBe allEvents.map(_.id))
      } yield res
    }
  }

  "returnToQueue" should {

    "change status of the given event to New" in {

      val event = dequeuedEvents.generateOne

      val repository = createRepository()

      dequeuer(repository).returnToQueue(event).assertNoException >>
        repository.returnedToQueue.get.asserting(_ shouldBe List(event.id))
    }
  }

  private lazy val category = categoryNames.generateOne

  private val categoryLock: Lock[IO, CategoryName] = Lock.none[IO, CategoryName]
  private def dequeuer(repository: Repository) = new EventsDequeuerImpl[IO, TestDB](repository, categoryLock)

  private def createRepository(eventBatches: List[DequeuedEvent]*) =
    new Repository(eventBatches.toList)

  private class Repository(initialEventBatches: List[List[DequeuedEvent]]) extends EventsRepository[IO] {

    override def insert(category: CategoryName, payload: Json): CommandDef[IO] = fail("Shouldn't be called")

    private val eventBatches = Ref.unsafe[IO, List[List[DequeuedEvent]]](initialEventBatches)

    def addEventBatch(batch: List[DequeuedEvent]): IO[Unit] =
      eventBatches.update(_ appended batch)

    override def fetchEvents(category: CategoryName, chunkSize: Int): QueryDef[IO, List[DequeuedEvent]] =
      QueryDef.liftF {
        eventBatches
          .modify {
            case Nil          => Nil  -> List.empty[DequeuedEvent]
            case head :: tail => tail -> head
          }
      }

    val processed = Ref.unsafe[IO, List[Int]](Nil)

    override def markUnderProcessing(eventId: Int): CommandDef[IO] =
      CommandDef.liftF(processed.update(_ appended eventId))

    val returnedToQueue = Ref.unsafe[IO, List[Int]](Nil)

    override def returnToQueue(eventId: Int): CommandDef[IO] =
      CommandDef.liftF(returnedToQueue.update(_ appended eventId))

    val deleted = Ref.unsafe[IO, List[Int]](Nil)

    override def delete(eventId: Int): CommandDef[IO] =
      CommandDef.liftF(deleted.update(_ appended eventId))
  }

  private class AccumulatingHandler() extends Pipe[IO, Chunk[DequeuedEvent], Unit] {

    val handledEvents = SignallingRef[IO, List[DequeuedEvent]](Nil).unsafeRunSync()

    override def apply(stream: Stream[IO, Chunk[DequeuedEvent]]): Stream[IO, Unit] =
      stream.evalMap(chunk => handledEvents.update(_ appendedAll chunk.toList))
  }
}
