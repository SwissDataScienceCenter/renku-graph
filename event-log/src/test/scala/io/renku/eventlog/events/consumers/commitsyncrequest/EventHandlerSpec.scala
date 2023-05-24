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

package io.renku.eventlog.events.consumers
package commitsyncrequest

import cats.effect.IO
import io.circe.syntax._
import io.renku.eventlog.api.events.Generators
import io.renku.events.EventRequestContent
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers with EitherValues {

  "createHandlingDefinition.decode" should {

    "decode project id and path" in new TestCase {

      val definition = handler.createHandlingDefinition()

      val event = Generators.commitSyncRequests.generateOne

      definition.decode(EventRequestContent(event.asJson)).value shouldBe event
    }
  }

  "createHandlingDefinition.process" should {
    "call commitSyncForcer" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val event      = Generators.commitSyncRequests.generateOne
      (commitSyncForcer.forceCommitSync _).expects(event.project.id, event.project.path).returning(IO.unit)
      definition.process(event).unsafeRunSync() shouldBe ()
    }
  }

  "createHandlingDefinition" should {
    "not define onRelease and precondition" in new TestCase {
      val definition = handler.createHandlingDefinition()
      definition.onRelease                    shouldBe None
      definition.precondition.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val commitSyncForcer = mock[CommitSyncForcer[IO]]
    val handler          = new EventHandler[IO](commitSyncForcer)
  }
}
