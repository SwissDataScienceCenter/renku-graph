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

package io.renku.entities.viewings.collector.projects.viewed

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Temporal}
import fs2.concurrent.SignallingRef
import fs2.{Chunk, Stream}
import io.renku.eventsqueue.Generators.dequeuedEvents
import io.renku.eventsqueue.{DequeuedEvent, EventsDequeuer}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class SubscriptionFactorySpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  "kickOffEventsDequeueing" should {

    "hook the processor into the stream acquired from the dequeuer" in {

      val chunk1 = dequeuedEvents.generateList()
      val chunk2 = dequeuedEvents.generateList()
      givenDequeuer(returning = Stream(Chunk.from(chunk1), Chunk.from(chunk2)))

      val eventProcessor = new AccumulatingHandler

      for {
        _ <- SubscriptionFactory.kickOffEventsDequeueing[IO](eventsDequeuer, eventProcessor).assertNoException
        _ <- eventProcessor.handledEvents.waitUntil(_ == chunk1 ::: chunk2)
      } yield Succeeded
    }

    "handle failures in the dequeueing process so the process does not die" in {

      val chunk1 = dequeuedEvents.generateList(min = 1)
      givenDequeuer(returning = Stream.emit(Chunk.from(chunk1)))
      val chunk2 = dequeuedEvents.generateList(min = 1)
      givenDequeuer(returning = Stream.emit(Chunk.from(chunk2)))

      val eventProcessor = new AccumulatingHandler(maybeEventToFail = chunk1.headOption)

      for {
        _ <- SubscriptionFactory.kickOffEventsDequeueing[IO](eventsDequeuer, eventProcessor).assertNoException
        _ <- IO.race(
               eventProcessor.handledEvents.waitUntil(_ == chunk2),
               Temporal[IO].delayBy(IO(fail("Events dequeuer process doesn't work")), 5 seconds)
             )
      } yield Succeeded
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val eventsDequeuer = mock[EventsDequeuer[IO]]

  private def givenDequeuer(returning: Stream[IO, Chunk[DequeuedEvent]]) =
    (eventsDequeuer.acquireEventsStream _)
      .expects(categoryName)
      .returning(returning)

  private class AccumulatingHandler(maybeEventToFail: Option[DequeuedEvent] = None) extends EventProcessor[IO] {

    val handledEvents = SignallingRef[IO, List[DequeuedEvent]](Nil).unsafeRunSync()

    override def apply(stream: Stream[IO, Chunk[DequeuedEvent]]): Stream[IO, Unit] =
      stream.evalMap {
        case chunk if maybeEventToFail exists chunk.toList.contains => throw exceptions.generateOne
        case chunk                                                  => handledEvents.update(_ appendedAll chunk.toList)
      }
  }
}
