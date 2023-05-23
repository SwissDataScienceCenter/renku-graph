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

package io.renku.webhookservice
package eventstatus

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.eventlog
import io.renku.eventlog.api.events.CommitSyncRequest
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.ErrorMessage._
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.webhookservice.eventstatus.Generators._
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec with ScalaCheckPropertyChecks {

  "fetchProcessingStatus" should {

    "return OK with project status info if webhook for the project exists" in new TestCase {

      val authUser = authUsers.generateOption

      givenHookValidation(projectId, authUser, returning = HookExists.some.pure[IO])

      val statusInfo = statusInfos.generateOne
      givenStatusInfoFinding(projectId, returning = statusInfo.some.pure[IO])

      val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe statusInfo.asJson

      logger.loggedOnly(
        Warn(s"Finding status info for project '$projectId' finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "send COMMIT_SYNC_REQUEST message and return OK with activated = true and status 'in-progress' " +
      "if webhook for the project exists but no status info can be found in EL" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = HookExists.some.pure[IO])
        givenStatusInfoFinding(projectId, returning = None.pure[IO])

        val project = consumerProjects.generateOne.copy(id = projectId)
        givenProjectInfoFinding(projectId, authUser, returning = project.pure[IO])
        givenCommitSyncRequestSend(project, returning = ().pure[IO])

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                   shouldBe Ok
        response.contentType              shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe StatusInfo.webhookReady.asJson

        logger.loggedOnly(
          Warn(s"Finding status info for project '$projectId' finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    "return OK with activated = false if the webhook does not exist" in new TestCase {

      val authUser = authUsers.generateOption

      givenHookValidation(projectId, authUser, returning = HookMissing.some.pure[IO])
      givenStatusInfoFinding(projectId, returning = statusInfos.generateOption.pure[IO])

      val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe StatusInfo.NotActivated.asJson
    }

    "return OK with project status info with activated = true " +
      "if determining project hook existence returns None " +
      "but status info can be found in EL" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = None.pure[IO])

        val statusInfo = statusInfos.generateOne
        givenStatusInfoFinding(projectId, returning = statusInfo.some.pure[IO])

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                   shouldBe Ok
        response.contentType              shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe statusInfo.asJson
      }

    "return NOT_FOUND if determining project hook existence returns None " +
      "and no status info can be found in EL" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = None.pure[IO])
        givenStatusInfoFinding(projectId, returning = None.pure[IO])

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                   shouldBe NotFound
        response.contentType              shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe InfoMessage("Info about project cannot be found").asJson
      }

    "return INTERNAL_SERVER_ERROR when checking if project webhook exists fails" in new TestCase {

      val authUser = authUsers.generateOption

      val exception = exceptions.generateOne
      givenHookValidation(projectId, authUser, returning = exception.raiseError[IO, Nothing])
      givenStatusInfoFinding(projectId, returning = statusInfos.generateOption.pure[IO])

      val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(statusInfoFindingErrorMessage).asJson

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }

    "return INTERNAL_SERVER_ERROR when finding status info returns a failure" in new TestCase {

      val authUser = authUsers.generateOption

      givenHookValidation(projectId, authUser, returning = hookValidationResults.generateOption.pure[IO])

      val exception = exceptions.generateOne
      givenStatusInfoFinding(projectId, returning = exception.raiseError[IO, Nothing])

      val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(statusInfoFindingErrorMessage).asJson

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }
  }

  private trait TestCase {

    val projectId = projectIds.generateOne

    private val hookValidator     = mock[HookValidator[IO]]
    private val statusInfoFinder  = mock[StatusInfoFinder[IO]]
    private val projectInfoFinder = mock[ProjectInfoFinder[IO]]
    private val elClient          = mock[eventlog.api.events.Client[IO]]
    implicit val logger:                TestLogger[IO]                = TestLogger[IO]()
    implicit val executionTimeRecorder: TestExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    val endpoint = new EndpointImpl[IO](hookValidator, statusInfoFinder, projectInfoFinder, elClient)

    lazy val statusInfoFindingErrorMessage = show"Finding status info for project '$projectId' failed"

    def givenHookValidation(projectId: projects.GitLabId,
                            authUser:  Option[AuthUser],
                            returning: IO[Option[HookValidator.HookValidationResult]]
    ) = (hookValidator
      .validateHook(_: GitLabId, _: Option[AccessToken]))
      .expects(projectId, authUser.map(_.accessToken))
      .returning(returning)

    def givenStatusInfoFinding(projectId: projects.GitLabId, returning: IO[Option[StatusInfo]]) =
      (statusInfoFinder
        .findStatusInfo(_: GitLabId))
        .expects(projectId)
        .returning(returning)

    def givenProjectInfoFinding(projectId: projects.GitLabId, authUser: Option[AuthUser], returning: IO[Project]) =
      (projectInfoFinder
        .findProjectInfo(_: projects.GitLabId)(_: Option[AccessToken]))
        .expects(projectId, authUser.map(_.accessToken))
        .returning(returning)

    def givenCommitSyncRequestSend(project: Project, returning: IO[Unit]) =
      (elClient
        .send(_: CommitSyncRequest))
        .expects(CommitSyncRequest(project))
        .returning(returning)
  }
}
