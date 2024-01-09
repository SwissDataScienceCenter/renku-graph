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

package io.renku.webhookservice
package eventstatus

import cats.effect.{IO, Ref}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.data.Message
import io.renku.eventlog.api.events.CommitSyncRequest
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.{ProjectViewedEvent, UserId}
import io.renku.webhookservice.eventstatus.Generators._
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.{eventlog, triplesgenerator}
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.http4s.circe.CirceEntityCodec._
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with IOSpec
    with Eventually
    with IntegrationPatience {

  "fetchProcessingStatus" should {

    "return OK with project status info and send PROJECT_VIEWED_EVENT " +
      "if webhook for the project exists" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = HookExists.some.pure[IO])

        val statusInfo = statusInfos.generateOne
        givenStatusInfoFinding(projectId, returning = statusInfo.some.pure[IO])

        val project = consumerProjects.generateOne.copy(id = projectId)
        givenProjectInfoFinding(projectId, authUser, returning = project.some.pure[IO])
        givenProjectViewedEventSent

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                   shouldBe Ok
        response.contentType              shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe statusInfo.asJson

        verifyProjectViewedEventSent(project, authUser)

        logger.loggedOnly(
          Warn(s"Finding status info for project '$projectId' finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    "send COMMIT_SYNC_REQUEST and return OK with activated = true and status 'in-progress' " +
      "if webhook for the project exists but no status info can be found in EL" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = HookExists.some.pure[IO])
        givenStatusInfoFinding(projectId, returning = None.pure[IO])

        val project = consumerProjects.generateOne.copy(id = projectId)
        givenProjectInfoFinding(projectId, authUser, returning = project.some.pure[IO])

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                   shouldBe Ok
        response.contentType              shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe StatusInfo.webhookReady.asJson

        elClient.waitForArrival(CommitSyncRequest(project)).unsafeRunSync()

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

    "return OK with project status info with activated = true and " +
      "send PROJECT_VIEWED_EVENT " +
      "if determining project hook existence returns None " +
      "but status info can be found in EL" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = None.pure[IO])

        val statusInfo = statusInfos.generateOne
        givenStatusInfoFinding(projectId, returning = statusInfo.some.pure[IO])

        val project = consumerProjects.generateOne.copy(id = projectId)
        givenProjectInfoFinding(projectId, authUser, returning = project.some.pure[IO])
        givenProjectViewedEventSent

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                   shouldBe Ok
        response.contentType              shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe statusInfo.asJson

        verifyProjectViewedEventSent(project, authUser)
      }

    "return NOT_FOUND if determining project hook existence returns None " +
      "and no status info can be found in EL" in new TestCase {

        val authUser = authUsers.generateOption

        givenHookValidation(projectId, authUser, returning = None.pure[IO])
        givenStatusInfoFinding(projectId, returning = None.pure[IO])

        val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

        response.status                      shouldBe NotFound
        response.contentType                 shouldBe Some(`Content-Type`(application.json))
        response.as[Message].unsafeRunSync() shouldBe Message.Info("Info about project cannot be found")
      }

    "return INTERNAL_SERVER_ERROR when checking if project webhook exists fails" in new TestCase {

      val authUser = authUsers.generateOption

      val exception = exceptions.generateOne
      givenHookValidation(projectId, authUser, returning = exception.raiseError[IO, Nothing])
      givenStatusInfoFinding(projectId, returning = statusInfos.generateOption.pure[IO])

      val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

      response.status                      shouldBe InternalServerError
      response.contentType                 shouldBe Some(`Content-Type`(application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Error.unsafeApply(statusInfoFindingErrorMessage)

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }

    "return INTERNAL_SERVER_ERROR when finding status info returns a failure" in new TestCase {

      val authUser = authUsers.generateOption

      givenHookValidation(projectId, authUser, returning = hookValidationResults.generateOption.pure[IO])

      val exception = exceptions.generateOne
      givenStatusInfoFinding(projectId, returning = exception.raiseError[IO, Nothing])

      val response = endpoint.fetchProcessingStatus(projectId, authUser).unsafeRunSync()

      response.status                      shouldBe InternalServerError
      response.contentType                 shouldBe Some(`Content-Type`(application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Error.unsafeApply(statusInfoFindingErrorMessage)

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }
  }

  private trait TestCase {

    val projectId = projectIds.generateOne

    private val hookValidator     = mock[HookValidator[IO]]
    private val statusInfoFinder  = mock[StatusInfoFinder[IO]]
    private val projectInfoFinder = mock[ProjectInfoFinder[IO]]
    val elClient                  = eventlog.api.events.TestClient.collectingMode[IO]
    private val tgClient          = mock[triplesgenerator.api.events.Client[IO]]
    implicit val logger:                TestLogger[IO]                = TestLogger[IO]()
    implicit val executionTimeRecorder: TestExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    val endpoint = new EndpointImpl[IO](hookValidator, statusInfoFinder, projectInfoFinder, elClient, tgClient)

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

    def givenProjectInfoFinding(projectId: projects.GitLabId,
                                authUser:  Option[AuthUser],
                                returning: IO[Option[Project]]
    ) = (projectInfoFinder
      .findProjectInfo(_: projects.GitLabId)(_: Option[AccessToken]))
      .expects(projectId, authUser.map(_.accessToken))
      .returning(returning)

    private val sentEvents: Ref[IO, List[AnyRef]] = Ref.unsafe(Nil)

    def givenProjectViewedEventSent =
      (tgClient
        .send(_: ProjectViewedEvent))
        .expects(*)
        .onCall((ev: ProjectViewedEvent) => sentEvents.update(ev :: _))

    def verifyProjectViewedEventSent(project: Project, authUser: Option[AuthUser]) = eventually {
      sentEvents.get.unsafeRunSync().collect { case e: ProjectViewedEvent => e.slug -> e.maybeUserId } shouldBe
        List(project.slug -> authUser.map(_.id).map(UserId(_)))
    }
  }
}
