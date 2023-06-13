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
package zombieevents

import Generators.zombieEvents
import cats.effect._
import cats.syntax.all._
import io.renku.eventlog.events.producers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventsFinderSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "popEvent" should {

    "returns the event if found by the LongProcessingEventFinder" in new TestCase {
      val zombieEvent = zombieEvents.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[IO])
      (longProcessingEventsFinder.popEvent _).expects().returning(zombieEvent.some.pure[IO])

      zombieEventFinder.popEvent().unsafeRunSync() shouldBe zombieEvent.some
    }

    "returns the event if found by the LostSubscriberEventFinder" in new TestCase {
      val zombieEvent = zombieEvents.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[IO])
      (longProcessingEventsFinder.popEvent _).expects().returning(None.pure[IO])
      (lostSubscriberEventsFinder.popEvent _).expects().returning(zombieEvent.some.pure[IO])

      zombieEventFinder.popEvent().unsafeRunSync() shouldBe zombieEvent.some
    }

    "returns the potential event found by the LostZombieEventFinder" in new TestCase {
      val maybeZombieEvent = zombieEvents.generateOption
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[IO])
      (longProcessingEventsFinder.popEvent _).expects().returning(None.pure[IO])
      (lostSubscriberEventsFinder.popEvent _).expects().returning(None.pure[IO])
      (lostZombieEventsFinder.popEvent _).expects().returning(maybeZombieEvent.pure[IO])

      zombieEventFinder.popEvent().unsafeRunSync() shouldBe maybeZombieEvent
    }

    "fail if the LongProcessingEventFinder fails" in new TestCase {
      val exception = exceptions.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[IO])
      (longProcessingEventsFinder.popEvent _).expects().returning(exception.raiseError[IO, Option[ZombieEvent]])

      val ex =
        intercept[Exception] {
          zombieEventFinder.popEvent().unsafeRunSync()
        }

      ex shouldBe exception
    }

    "continue if the ZombieEventSourceCleaner fails" in new TestCase {
      val exception   = exceptions.generateOne
      val zombieEvent = zombieEvents.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(exception.raiseError[IO, Unit])
      (longProcessingEventsFinder.popEvent _).expects().returning(zombieEvent.some.pure[IO])

      zombieEventFinder.popEvent().unsafeRunSync() shouldBe zombieEvent.some

      logger.loggedOnly(Error("ZombieEventSourceCleaner - failure during clean up", exception))
    }
  }

  private trait TestCase {
    val longProcessingEventsFinder = mock[producers.EventFinder[IO, ZombieEvent]]
    val lostSubscriberEventsFinder = mock[producers.EventFinder[IO, ZombieEvent]]
    val zombieNodesCleaner         = mock[ZombieNodesCleaner[IO]]
    val lostZombieEventsFinder     = mock[producers.EventFinder[IO, ZombieEvent]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val zombieEventFinder = new EventFinder[IO](longProcessingEventsFinder,
                                                lostSubscriberEventsFinder,
                                                zombieNodesCleaner,
                                                lostZombieEventsFinder
    )
  }
}
