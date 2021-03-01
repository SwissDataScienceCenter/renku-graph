/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.zombieevents

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.subscriptions.zombieevents.Generators.zombieEvents
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ZombieEventsFinderSpec extends AnyWordSpec with MockFactory with should.Matchers {
  "popEvent" should {
    "returns the event found by the LongProcessingEventFinder" in new TestCase {
      (longProcessingEventsFinder.popEvent _).expects().returning(maybeZombieEvent.pure[Try])

      zombieEventFinder.popEvent() shouldBe maybeZombieEvent.pure[Try]
    }
  }
  private trait TestCase {
    val maybeZombieEvent           = zombieEvents.generateOption
    val longProcessingEventsFinder = mock[EventFinder[Try, ZombieEvent]]
    val zombieEventFinder          = new ZombieEventFinder[Try](longProcessingEventsFinder)
  }

}
