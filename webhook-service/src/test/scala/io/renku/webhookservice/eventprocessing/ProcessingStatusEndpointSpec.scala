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

package io.renku.webhookservice
package eventprocessing

import Generators._
import cats.data.EitherT
import cats.data.EitherT.{leftT, right, rightT}
import cats.effect.IO
import cats.syntax.all._
import hookvalidation.HookValidator
import hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import hookvalidation.HookValidator.NoAccessTokenException
import io.circe.Json
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProcessingStatusEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "fetchProcessingStatus" should {

    "return OK with the status info if webhook for the project exists" in new TestCase {

      givenHookValidation(projectId, returning = HookExists.pure[IO])

      val statusInfo = statusInfos.generateOne
      givenStatusInfoFinding(projectId, returning = rightT(statusInfo))

      val response = endpoint.fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe statusInfo.asJson

      logger.loggedOnly(
        Warn(s"Finding status info for project '$projectId' finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "return OK with activated = false if the webhook does not exist" in new TestCase {

      givenHookValidation(projectId, returning = HookMissing.pure[IO])

      val response = endpoint.fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe StatusInfo.NotActivated.asJson
    }

    "return INTERNAL_SERVER_ERROR if no Access Token found" in new TestCase {

      val exception = NoAccessTokenException("error")
      givenHookValidation(projectId, returning = exception.raiseError[IO, HookValidator.HookValidationResult])

      val response = endpoint.fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(statusInfoFindingErrorMessage).asJson

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }

    "return INTERNAL_SERVER_ERROR when checking if the webhook exists fails" in new TestCase {

      val exception = exceptions.generateOne
      givenHookValidation(projectId, returning = exception.raiseError[IO, HookValidator.HookValidationResult])

      val response = endpoint.fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(statusInfoFindingErrorMessage).asJson

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }

    "return INTERNAL_SERVER_ERROR when finding status returns a failure" in new TestCase {

      givenHookValidation(projectId, returning = HookExists.pure[IO])

      val exception = exceptions.generateOne
      givenStatusInfoFinding(projectId, returning = leftT(exception))

      val response = endpoint.fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(statusInfoFindingErrorMessage).asJson

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }

    "return INTERNAL_SERVER_ERROR when finding status info fails" in new TestCase {

      givenHookValidation(projectId, returning = HookExists.pure[IO])

      val exception = exceptions.generateOne
      givenStatusInfoFinding(projectId, returning = right(exception.raiseError[IO, StatusInfo]))

      val response = endpoint.fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(statusInfoFindingErrorMessage).asJson

      logger.logged(Error(statusInfoFindingErrorMessage, exception))
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    private val hookValidator    = mock[HookValidator[IO]]
    private val statusInfoFinder = mock[StatusInfoFinder[IO]]
    implicit val logger:                TestLogger[IO]                = TestLogger[IO]()
    implicit val executionTimeRecorder: TestExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    val endpoint = new ProcessingStatusEndpointImpl[IO](hookValidator, statusInfoFinder)

    lazy val statusInfoFindingErrorMessage = show"Finding status info for project '$projectId' failed"

    def givenHookValidation(projectId: projects.GitLabId, returning: IO[HookValidator.HookValidationResult]) =
      (hookValidator
        .validateHook(_: GitLabId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(returning)

    def givenStatusInfoFinding(projectId: projects.GitLabId, returning: EitherT[IO, Throwable, StatusInfo]) =
      (statusInfoFinder
        .findStatusInfo(_: GitLabId))
        .expects(projectId)
        .returning(returning)
  }
}
