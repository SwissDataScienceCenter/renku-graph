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

package io.renku.eventlog.events.categories.statuschange

import Generators.projectEventToNewEvents
import cats.Show
import cats.data.Kleisli
import cats.syntax.all._
import io.circe.Encoder
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.categories.statuschange.StatusChangeEventsQueue.EventType
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.Session

import scala.util.Try

class ProjectEventsToNewHandlerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "updateDB" should {

    "offer the ProjectEventsToNew event to the queue" in new TestCase {
      (queue
        .offer(_: ProjectEventsToNew)(_: Encoder[ProjectEventsToNew],
                                      _: EventType[ProjectEventsToNew],
                                      _: Show[ProjectEventsToNew]
        ))
        .expects(event, *, *, *)
        .returning(Kleisli.pure(()))

      handler.updateDB(event)(session) shouldBe DBUpdateResults.ForProjects.empty.pure[Try]
    }
  }

  "onRollback" should {
    "return unit Kleisli" in new TestCase {
      handler.onRollback(event)(session) shouldBe ().pure[Try]
    }
  }

  private trait TestCase {
    val event   = projectEventToNewEvents.generateOne
    val session = mock[Session[Try]]

    val queue   = mock[StatusChangeEventsQueue[Try]]
    val handler = new ProjectEventsToNewHandler[Try](queue)
  }
}
