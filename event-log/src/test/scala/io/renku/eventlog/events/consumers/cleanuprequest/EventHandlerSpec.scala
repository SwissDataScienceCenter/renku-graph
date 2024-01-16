/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.api.events.CleanUpRequest
import io.renku.events.EventRequestContent
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {
  val fullEvent:    CleanUpRequest       = CleanUpRequest.Full(projectIds.generateOne, projectSlugs.generateOne)
  val partialEvent: CleanUpRequest       = CleanUpRequest.Partial(projectSlugs.generateOne)
  val events:       List[CleanUpRequest] = List(fullEvent, partialEvent)

  "createHandlingDefinition.decode" should {
    events.foreach { event =>
      s"decode valid event data: $event" in new TestCase {
        val definition = handler.createHandlingDefinition()
        definition.decode(EventRequestContent(event.asJson)) shouldBe event.asRight
      }
    }

    "fail on malformed event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Json.obj("invalid" -> true.asJson)
      definition.decode(EventRequestContent(eventData)).isLeft shouldBe true
    }
  }

  "createHandlingDefinition.process" should {
    "call event processor" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (processor.process _).expects(*).returning(IO.unit)
      definition.process(fullEvent).unsafeRunSync() shouldBe ()
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
    val processor = mock[EventProcessor[IO]]
    val handler   = new EventHandler[IO](processor)
  }
}
