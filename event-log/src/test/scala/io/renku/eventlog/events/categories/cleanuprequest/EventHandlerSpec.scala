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

package io.renku.eventlog.events.categories.cleanuprequest

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest, SchedulingError}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "tryHandling" should {

    "decode an event from the request, pass it to the queue and return Accepted" in new TestCase {
      (eventsQueue.offer _).expects(projectPath).returning(().pure[IO])

      handler.tryHandling(event).unsafeRunSync() shouldBe Accepted

      logger.loggedOnly(Info(show"$categoryName: projectPath = $projectPath -> $Accepted"))
    }

    "fail with BadRequest for malformed event" in new TestCase {

      handler
        .tryHandling(EventRequestContent.NoPayload(json"""{"categoryName": "CLEAN_UP_REQUEST"}"""))
        .unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    "log an error if offering the event to the queue fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventsQueue.offer _).expects(projectPath).returning(exception.raiseError[IO, Unit])

      handler.tryHandling(event).unsafeRunSync() shouldBe SchedulingError(exception)

      logger.loggedOnly(Error(show"$categoryName: projectPath = $projectPath -> SchedulingError", exception))
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventsQueue = mock[CleanUpEventsQueue[IO]]
    val handler     = new EventHandler[IO](eventsQueue)

    lazy val event = EventRequestContent.NoPayload(json"""{
      "categoryName": "CLEAN_UP_REQUEST",
      "project": {
        "path": ${projectPath.value}
      }
    }""")
  }
}
