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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest, SchedulingError}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event containing project id and path from the request, " +
      "kick-off passing the event to the queue " +
      "and return Accepted" in new TestCase {
        val projectId = projectIds.generateOne
        val event     = CleanUpRequestEvent(projectId, projectPath)
        (processor.process _).expects(event).returning(().pure[IO])

        handler.tryHandling(eventRequest(projectId, projectPath)).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
      }

    "decode an event containing project path only from the request, " +
      "kick-off passing the event to the queue " +
      "and return Accepted" in new TestCase {
        val event = CleanUpRequestEvent(projectPath)
        (processor.process _).expects(event).returning(().pure[IO])

        handler.tryHandling(eventRequest(projectPath)).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))
      }

    "fail with BadRequest for malformed event" in new TestCase {

      handler
        .tryHandling(EventRequestContent.NoPayload(json"""{"categoryName": "CLEAN_UP_REQUEST"}"""))
        .unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    "log an error if processing the event fails" in new TestCase {
      val projectId = projectIds.generateOne
      val event     = CleanUpRequestEvent(projectId, projectPath)

      val exception = exceptions.generateOne
      (processor.process _).expects(event).returning(exception.raiseError[IO, Unit])

      handler.tryHandling(eventRequest(projectId, projectPath)).unsafeRunSync() shouldBe SchedulingError(exception)

      logger.loggedOnly(Error(show"$categoryName: $event -> SchedulingError", exception))
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val processor = mock[EventProcessor[IO]]
    val handler   = new EventHandler[IO](processor)

    def eventRequest(projectId: projects.Id, projectPath: projects.Path) = EventRequestContent.NoPayload(json"""{
      "categoryName": "CLEAN_UP_REQUEST",
      "project": {
        "id":   ${projectId.value},
        "path": ${projectPath.show}
      }
    }""")

    def eventRequest(projectPath: projects.Path) = EventRequestContent.NoPayload(json"""{
      "categoryName": "CLEAN_UP_REQUEST",
      "project": {
        "path": ${projectPath.show}
      }
    }""")
  }
}
