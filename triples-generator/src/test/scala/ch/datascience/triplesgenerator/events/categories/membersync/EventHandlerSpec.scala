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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.effect.IO
import cats.effect.concurrent.Deferred
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule members sync " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        (membersSynchronizer.synchronizeMembers _)
          .expects(projectPath)
          .returning(IO.unit)

        val request = requestContent(projectPath.asJson(eventEncoder))

        handler.handle(request).getResult shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: projectPath = $projectPath -> $Accepted"
          )
        )
      }

    s"return $BadRequest if project path is malformed" in new TestCase {

      val request = requestContent(json"""{
        "categoryName": "MEMBER_SYNC",
        "project": {
          "path" :      ${nonNegativeInts().generateOne.value}
        }
      }""")

      handler.handle(request).getResult shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val membersSynchronizer = mock[MembersSynchronizer[IO]]
    val logger              = TestLogger[IO]()
    val handler             = new EventHandler[IO](categoryName, membersSynchronizer, logger)

    def requestContent(event: Json): EventRequestContent = EventRequestContent(event, None)
  }

  implicit lazy val eventEncoder: Encoder[projects.Path] =
    Encoder.instance[projects.Path] { projectPath =>
      json"""{
        "categoryName": "MEMBER_SYNC",
        "project": {
          "path" :      ${projectPath.value}
        }
      }"""
    }

  private implicit class HandlerOps(handlerResult: IO[(Deferred[IO, Unit], IO[EventSchedulingResult])]) {
    lazy val getResult = handlerResult.unsafeRunSync()._2.unsafeRunSync()
  }
}
