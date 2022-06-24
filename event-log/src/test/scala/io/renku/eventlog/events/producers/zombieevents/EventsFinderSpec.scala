/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.syntax.all._
import io.renku.eventlog.events.producers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class EventsFinderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "popEvent" should {

    "returns the event if found by the LongProcessingEventFinder" in new TestCase {
      val zombieEvent = zombieEvents.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[Try])
      (longProcessingEventsFinder.popEvent _).expects().returning(zombieEvent.some.pure[Try])

      zombieEventFinder.popEvent() shouldBe zombieEvent.some.pure[Try]
    }
    "returns the event if found by the LostSubscriberEventFinder" in new TestCase {
      val zombieEvent = zombieEvents.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[Try])
      (longProcessingEventsFinder.popEvent _).expects().returning(None.pure[Try])
      (lostSubscriberEventsFinder.popEvent _).expects().returning(zombieEvent.some.pure[Try])

      zombieEventFinder.popEvent() shouldBe zombieEvent.some.pure[Try]
    }

    "returns the potential event found by the LostZombieEventFinder" in new TestCase {
      val maybeZombieEvent = zombieEvents.generateOption
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[Try])
      (longProcessingEventsFinder.popEvent _).expects().returning(None.pure[Try])
      (lostSubscriberEventsFinder.popEvent _).expects().returning(None.pure[Try])
      (lostZombieEventsFinder.popEvent _).expects().returning(maybeZombieEvent.pure[Try])

      zombieEventFinder.popEvent() shouldBe maybeZombieEvent.pure[Try]
    }

    "fail if the LongProcessingEventFinder fails" in new TestCase {
      val exception = exceptions.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(().pure[Try])
      (longProcessingEventsFinder.popEvent _).expects().returning(exception.raiseError[Try, Option[ZombieEvent]])

      zombieEventFinder.popEvent() shouldBe exception.raiseError[Try, Option[ZombieEvent]]
    }

    "continue if the ZombieEventSourceCleaner fails" in new TestCase {
      val exception   = exceptions.generateOne
      val zombieEvent = zombieEvents.generateOne
      (zombieNodesCleaner.removeZombieNodes _).expects().returning(exception.raiseError[Try, Unit])
      (longProcessingEventsFinder.popEvent _).expects().returning(zombieEvent.some.pure[Try])

      zombieEventFinder.popEvent() shouldBe zombieEvent.some.pure[Try]

      logger.loggedOnly(Error("ZombieEventSourceCleaner - failure during clean up", exception))
    }
  }

  private trait TestCase {
    val longProcessingEventsFinder = mock[producers.EventFinder[Try, ZombieEvent]]
    val lostSubscriberEventsFinder = mock[producers.EventFinder[Try, ZombieEvent]]
    val zombieNodesCleaner         = mock[ZombieNodesCleaner[Try]]
    val lostZombieEventsFinder     = mock[producers.EventFinder[Try, ZombieEvent]]
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val zombieEventFinder = new EventFinder[Try](longProcessingEventsFinder,
                                                 lostSubscriberEventsFinder,
                                                 zombieNodesCleaner,
                                                 lostZombieEventsFinder
    )
  }
}
