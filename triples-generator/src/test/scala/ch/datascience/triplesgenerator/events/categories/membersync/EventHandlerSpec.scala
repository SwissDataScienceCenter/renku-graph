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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.triplesgenerator.events.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.syntax.all._
import org.http4s.{Method, Request}
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

        val request = Request[IO](Method.POST, uri"events").withEntity(projectPath.asJson(eventEncoder))

        handler.handle(request).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: projectPath = $projectPath -> $Accepted"
          )
        )
      }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      val payload = jsons.generateOne.asJson
      val request = Request(Method.POST, uri"events").withEntity(payload)

      handler.handle(request).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if project path is malformed" in new TestCase {

      val request = Request(Method.POST, uri"events").withEntity(json"""{
        "categoryName": "MEMBER_SYNC",
        "project": {
          "path" :      ${nonNegativeInts().generateOne.value}
        }
      }""")

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val membersSynchronizer = mock[MemberSynchronizer[IO]]
    val logger              = TestLogger[IO]()
    val handler             = new EventHandler[IO](membersSynchronizer, logger)
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
}
