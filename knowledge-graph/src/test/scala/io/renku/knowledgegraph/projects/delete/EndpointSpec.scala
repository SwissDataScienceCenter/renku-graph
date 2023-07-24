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

import cats.effect.{Deferred, IO}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.data.Message.Codecs._
import io.renku.eventlog.api.events.CommitSyncRequest
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.CleanUpEvent
import io.renku.{eventlog, triplesgenerator}
import org.http4s.MediaType.application
import org.http4s.Status.{Accepted, InternalServerError, NotFound}
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "DELETE /projects/:path" should {

    "fetch project details from GL, " +
      "call DELETE Project API on GL, " +
      "wait for the projects to be gone from GL, " +
      "send a COMMIT_SYNC_REQUEST event to EL and a CLEAN_UP event to TG " +
      "and return 202 Accepted" in new TestCase {

        givenProjectFindingInGL(project.path, returning = project.some.pure[IO])
        givenProjectDelete(project.id, returning = ().pure[IO])
        givenProjectFindingInGL(project.path, returning = None.pure[IO])
        givenCommitSyncRequestSent(project, returning = ().pure[IO])
        givenCleanUpRequestSent(project, returning = ().pure[IO])

        val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

        response.status                      shouldBe Accepted
        response.contentType                 shouldBe `Content-Type`(application.json).some
        response.as[Message].unsafeRunSync() shouldBe Message.Info("Project deleted")

        ensureCommitSyncSent.get.unsafeRunSync() shouldBe true
        ensureCleanUpSent.get.unsafeRunSync()    shouldBe true
      }

    "fetch project details from EL if it cannot be found in GL, " +
      "call DELETE Project API on GL, " +
      "wait for the projects to be gone from GL, " +
      "send a COMMIT_SYNC_REQUEST event to EL and a CLEAN_UP event to TG " +
      "and return 202 Accepted" in new TestCase {

        givenProjectFindingInGL(project.path, returning = None.pure[IO]).atLeastOnce()
        givenProjectFindingInEL(project.path, returning = project.some.pure[IO])
        givenProjectDelete(project.id, returning = ().pure[IO])
        givenCommitSyncRequestSent(project, returning = ().pure[IO])
        givenCleanUpRequestSent(project, returning = ().pure[IO])

        val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

        response.status                      shouldBe Accepted
        response.contentType                 shouldBe `Content-Type`(application.json).some
        response.as[Message].unsafeRunSync() shouldBe Message.Info("Project deleted")

        ensureCommitSyncSent.get.unsafeRunSync() shouldBe true
        ensureCleanUpSent.get.unsafeRunSync()    shouldBe true
      }

    "return 404 Not Found in case the project does not exist in GL" in new TestCase {

      givenProjectFindingInGL(project.path, returning = None.pure[IO])
      givenProjectFindingInEL(project.path, returning = None.pure[IO])

      val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

      response.status                      shouldBe NotFound
      response.contentType                 shouldBe `Content-Type`(application.json).some
      response.as[Message].unsafeRunSync() shouldBe Message.Info("Project does not exist")
    }

    "be sure the project gets deleted from GL before the COMMIT_SYNC_REQUEST event is sent to EL" in new TestCase {

      givenProjectFindingInGL(project.path, returning = project.some.pure[IO])
      givenProjectDelete(project.id, returning = ().pure[IO])
      givenProjectFindingInGL(project.path, returning = project.some.pure[IO])
      givenProjectFindingInGL(project.path, returning = None.pure[IO])
      givenCommitSyncRequestSent(project, returning = ().pure[IO])
      givenCleanUpRequestSent(project, returning = ().pure[IO])

      endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync().status shouldBe Accepted

      ensureCommitSyncSent.get.unsafeRunSync() shouldBe true
      ensureCleanUpSent.get.unsafeRunSync()    shouldBe true
    }

    "return 500 Internal Server Error in case any of the operations fails" in new TestCase {

      givenProjectFindingInGL(project.path, returning = project.some.pure[IO])
      val exception = exceptions.generateOne
      givenProjectDelete(project.id, returning = exception.raiseError[IO, Unit])

      val response = endpoint.`DELETE /projects/:path`(project.path, authUser).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe `Content-Type`(application.json).some
      response.as[Message].unsafeRunSync() shouldBe
        Message.Error.unsafeApply(s"Project deletion failure: ${exception.getMessage}")

      logger.loggedOnly(Error(s"Deleting '${project.path}' project failed", exception))
    }
  }

  private trait TestCase {

    val authUser = authUsers.generateOne
    val project  = consumerProjects.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val glProjectFinder = mock[GLProjectFinder[IO]]
    private val elProjectFinder = mock[ELProjectFinder[IO]]
    private val projectRemover  = mock[ProjectRemover[IO]]
    private val elClient        = mock[eventlog.api.events.Client[IO]]
    private val tgClient        = mock[triplesgenerator.api.events.Client[IO]]
    val endpoint = new EndpointImpl[IO](glProjectFinder, elProjectFinder, projectRemover, elClient, tgClient)

    def givenProjectFindingInGL(path: projects.Path, returning: IO[Option[Project]]) =
      (glProjectFinder
        .findProject(_: projects.Path)(_: AccessToken))
        .expects(path, authUser.accessToken)
        .returning(returning)

    def givenProjectFindingInEL(path: projects.Path, returning: IO[Option[Project]]) =
      (elProjectFinder
        .findProject(_: projects.Path))
        .expects(path)
        .returning(returning)

    def givenProjectDelete(id: projects.GitLabId, returning: IO[Unit]) =
      (projectRemover
        .deleteProject(_: projects.GitLabId)(_: AccessToken))
        .expects(id, authUser.accessToken)
        .returning(returning)

    val ensureCommitSyncSent = Deferred.unsafe[IO, Boolean]
    def givenCommitSyncRequestSent(project: Project, returning: IO[Unit]) =
      (elClient
        .send(_: CommitSyncRequest))
        .expects(CommitSyncRequest(project))
        .onCall((_: CommitSyncRequest) => returning >> ensureCommitSyncSent.complete(true).void)

    val ensureCleanUpSent = Deferred.unsafe[IO, Boolean]
    def givenCleanUpRequestSent(project: Project, returning: IO[Unit]) =
      (tgClient
        .send(_: CleanUpEvent))
        .expects(CleanUpEvent(project))
        .onCall((_: CleanUpEvent) => returning >> ensureCleanUpSent.complete(true).void)
  }
}
