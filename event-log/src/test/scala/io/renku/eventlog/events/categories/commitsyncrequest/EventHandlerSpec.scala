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

package io.renku.eventlog.events.categories
package commitsyncrequest

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, jsons}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "handle" should {

    s"decode an event from the request, " +
      "force commit sync " +
      s"and return $Accepted if forcing commit sync succeeds" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        (commitSyncForcer.forceCommitSync _)
          .expects(projectId, projectPath)
          .returning(().pure[IO])

        handler.handle(requestContent((projectId -> projectPath).asJson)).unsafeRunSync() shouldBe Accepted

        eventually {
          logger.loggedOnly(
            Info(
              s"${handler.categoryName}: projectId = $projectId, projectPath = $projectPath -> $Accepted"
            )
          )
        }
      }

    "log an error if commit sync forcing fails" in new TestCase {

      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne

      val exception = exceptions.generateOne
      (commitSyncForcer.forceCommitSync _)
        .expects(projectId, projectPath)
        .returning(exception.raiseError[IO, Unit])

      handler
        .handle(
          requestContent((projectId -> projectPath).asJson)
        )
        .unsafeRunSync() shouldBe SchedulingError(exception)

      eventually {
        logger.loggedOnly(
          Error(
            s"${handler.categoryName}: projectId = $projectId, projectPath = $projectPath -> SchedulingError",
            exception
          )
        )
      }
    }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      handler.handle(requestContent(jsons.generateOne.asJson)).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": "COMMIT_SYNC_REQUEST"
        }"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private trait TestCase {

    val commitSyncForcer = mock[CommitSyncForcer[IO]]
    val logger           = TestLogger[IO]()
    val handler          = new EventHandler[IO](categoryName, commitSyncForcer, logger)

  }

  private implicit lazy val eventEncoder: Encoder[(projects.Id, projects.Path)] =
    Encoder.instance[(projects.Id, projects.Path)] { case (id, path) =>
      json"""{
        "categoryName": "COMMIT_SYNC_REQUEST",
        "project": {
          "id":   ${id.value},
          "path": ${path.value}
        }
      }"""
    }
}
