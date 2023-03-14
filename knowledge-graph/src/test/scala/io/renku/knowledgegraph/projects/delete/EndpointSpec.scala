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

package io.renku.knowledgegraph.projects.delete

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.eventlog
import io.renku.graph.eventlog.api.events.CommitSyncRequest
import io.renku.graph.model.projects
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.http.ErrorMessage._
import io.renku.http.InfoMessage._
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.http4s.Status.{Accepted, InternalServerError, NotFound}
import org.http4s.headers.`Content-Type`
import org.http4s.MediaType.application
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "DELETE /projects/:path" should {

    "fetch project details from GL, " +
      "call DELETE Project API on GL, " +
      "wait for the projects to be gone from GL, " +
      "sent the COMMIT_SYNC_REQUEST event to EL " +
      "and return 202 Accepted" in new TestCase {

        givenProjectFinding(project.path, returning = project.some.pure[IO])
        givenProjectDelete(project.id, returning = ().pure[IO])
        givenProjectFinding(project.path, returning = None.pure[IO])
        givenCommitSyncRequestSent(project, returning = ().pure[IO])

        val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

        response.status                          shouldBe Accepted
        response.contentType                     shouldBe `Content-Type`(application.json).some
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Project deleted")
      }

    "return 404 Not Found in case the project does not exist in GL" in new TestCase {

      givenProjectFinding(project.path, returning = None.pure[IO])

      val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

      response.status                          shouldBe NotFound
      response.contentType                     shouldBe `Content-Type`(application.json).some
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Project does not exist")
    }

    "be sure the project gets deleted from GL before the COMMIT_SYNC_REQUEST event is sent to EL" in new TestCase {

      givenProjectFinding(project.path, returning = project.some.pure[IO])
      givenProjectDelete(project.id, returning = ().pure[IO])
      givenProjectFinding(project.path, returning = project.some.pure[IO])
      givenProjectFinding(project.path, returning = None.pure[IO])
      givenCommitSyncRequestSent(project, returning = ().pure[IO])

      endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync().status shouldBe Accepted
    }

    "return 500 Internal Server Error in case any of the operations fails" in new TestCase {

      givenProjectFinding(project.path, returning = project.some.pure[IO])
      val exception = exceptions.generateOne
      givenProjectDelete(project.id, returning = exception.raiseError[IO, Unit])

      val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe `Content-Type`(application.json).some
      response.as[ErrorMessage].unsafeRunSync() shouldBe
        ErrorMessage(s"Project deletion failure: ${exception.getMessage}")

      logger.loggedOnly(Error(s"Deleting '${project.path}' project failed", exception))
    }
  }

  private trait TestCase {

    val authUser = authUsers.generateOne
    val project  = consumerProjects.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val projectFinder  = mock[ProjectFinder[IO]]
    private val projectRemover = mock[ProjectRemover[IO]]
    private val elClient       = mock[eventlog.api.events.Client[IO]]
    val endpoint               = new EndpointImpl[IO](projectFinder, projectRemover, elClient)

    def givenProjectFinding(path: projects.Path, returning: IO[Option[Project]]) =
      (projectFinder.findProject _).expects(path).returning(returning)

    def givenProjectDelete(id: projects.GitLabId, returning: IO[Unit]) =
      (projectRemover.deleteProject _).expects(id).returning(returning)

    def givenCommitSyncRequestSent(project: Project, returning: IO[Unit]) =
      (elClient
        .send(_: CommitSyncRequest))
        .expects(CommitSyncRequest(project))
        .returning(returning)
  }
}
