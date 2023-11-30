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
import cats.effect.{IO, Ref, Temporal}
import fs2.concurrent.SignallingRef
import cats.syntax.all._
import fs2.{Chunk, Stream}
import io.renku.events.CategoryName
import io.renku.eventsqueue.Generators.dequeuedEvents
import io.renku.eventsqueue.{DequeuedEvent, EventsQueue}
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

    "wait with hooking the processor into the stream from the dequeuer until the TS state is not ready" in {

      val tsReadyResponses: Ref[IO, List[Boolean]] = Ref.unsafe(List(false, false, true))
      val tsReadyCheck: IO[Boolean] = tsReadyResponses.modify(resps => resps.tail -> resps.headOption.getOrElse(true))

      val chunk = dequeuedEvents.generateList(min = 1)
      givenStreamAcquiring(returning = Stream(Chunk.from(chunk)))

      for {
        processor <- newAccumulatingHandler()
        _ <- SubscriptionFactory
               .kickOffEventsDequeueing[IO](tsReadyCheck, eventsQueue, processor, onTSNotReadyWait = 100 millis)
               .assertNoException
        _   <- processor.handledEvents.waitUntil(_ == chunk)
        res <- tsReadyResponses.get.asserting(_.isEmpty shouldBe true)
      } yield res
    }

    "hook the processor into the stream acquired from the dequeuer" in {

      val chunk1 = dequeuedEvents.generateList(min = 1)
      val chunk2 = dequeuedEvents.generateList(min = 1)
      givenStreamAcquiring(returning = Stream(Chunk.from(chunk1), Chunk.from(chunk2)))

      for {
        processor <- newAccumulatingHandler()
        _         <- SubscriptionFactory.kickOffEventsDequeueing[IO](IO(true), eventsQueue, processor).assertNoException
        _         <- processor.handledEvents.waitUntil(_ == chunk1 ::: chunk2)
      } yield Succeeded
    }

    "handle failures in the dequeueing process so the process does not die" in {

      val chunk1 = dequeuedEvents.generateList(min = 1)
      givenStreamAcquiring(returning = Stream.emit(Chunk.from(chunk1)))
      val chunk2 = dequeuedEvents.generateList(min = 1)
      givenStreamAcquiring(returning = Stream.emit(Chunk.from(chunk2)))

      for {
        processor <- newAccumulatingHandler(maybeEventToFail = chunk1.headOption)
        _ <- SubscriptionFactory
               .kickOffEventsDequeueing[IO](IO(true), eventsQueue, processor, onErrorWait = 100 millis)
               .assertNoException
        _ <- IO.race(
               processor.handledEvents.waitUntil(_ == chunk2),
               Temporal[IO].delayBy(IO(fail("Events dequeuer process doesn't work")), 1 second)
             )
      } yield Succeeded
    }

    "handle failures happening while checking the TS state" in {

      val tsReadyResponses: Ref[IO, List[IO[Boolean]]] =
        Ref.unsafe(List(exceptions.generateOne.raiseError[IO, Boolean], true.pure[IO]))
      val tsReadyCheck: IO[Boolean] =
        tsReadyResponses.modify(resps => resps.tail -> resps.headOption.getOrElse(true.pure[IO])).flatten

      val chunk = dequeuedEvents.generateList(min = 1)
      givenStreamAcquiring(returning = Stream(Chunk.from(chunk)))

      for {
        processor <- newAccumulatingHandler()
        _ <- SubscriptionFactory
               .kickOffEventsDequeueing[IO](tsReadyCheck, eventsQueue, processor, onErrorWait = 100 millis)
               .assertNoException
        _   <- processor.handledEvents.waitUntil(_ == chunk)
        res <- tsReadyResponses.get.asserting(_.isEmpty shouldBe true)
      } yield res
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val eventsQueue = mock[EventsQueue[IO]]

  private def givenStreamAcquiring(returning: Stream[IO, Chunk[DequeuedEvent]]) =
    (eventsQueue
      .acquireEventsStream(_: CategoryName))
      .expects(categoryName)
      .returning(returning)

  private def newAccumulatingHandler(maybeEventToFail: Option[DequeuedEvent] = None): IO[AccumulatingHandler] =
    SignallingRef[IO, List[DequeuedEvent]](Nil).map(new AccumulatingHandler(maybeEventToFail, _))

  private class AccumulatingHandler(maybeEventToFail:  Option[DequeuedEvent],
                                    val handledEvents: SignallingRef[IO, List[DequeuedEvent]]
  ) extends EventProcessor[IO] {

    override def apply(stream: Stream[IO, Chunk[DequeuedEvent]]): Stream[IO, Unit] =
      stream.evalMap {
        case chunk if maybeEventToFail exists chunk.toList.contains => throw exceptions.generateOne
        case chunk                                                  => handledEvents.update(_ appendedAll chunk.toList)
      }
  }
}
