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

package io.renku.eventlog.events.categories
package commitsyncrequest

import cats.effect.IO
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
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

        handler
          .createHandlingProcess(requestContent((projectId -> projectPath).asJson))
          .unsafeRunSync()
          .process
          .value
          .unsafeRunSync() shouldBe Right(
          Accepted
        )

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
        .createHandlingProcess(
          requestContent((projectId -> projectPath).asJson)
        )
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Left(SchedulingError(exception))

      eventually {
        logger.loggedOnly(
          Error(
            s"${handler.categoryName}: projectId = $projectId, projectPath = $projectPath -> SchedulingError",
            exception
          )
        )
      }
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": "COMMIT_SYNC_REQUEST"
        }"""
      }

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val commitSyncForcer = mock[CommitSyncForcer[IO]]
    val handler          = new EventHandler[IO](categoryName, commitSyncForcer)
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
