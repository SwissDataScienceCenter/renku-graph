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

package io.renku.webhookservice.eventprocessing

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.IO
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects.Id
import io.renku.http.InfoMessage._
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus
import io.renku.webhookservice.eventprocessing.ProcessingStatusGenerator._
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.hookvalidation.HookValidator.NoAccessTokenException
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProcessingStatusEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "fetchProcessingStatus" should {

    "return OK with the progress status for the given projectId if the webhook exists" in new TestCase {

      (hookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookExists))

      val processingStatus = processingStatuses.generateOne
      (processingStatusFetcher
        .fetchProcessingStatus(_: Id))
        .expects(projectId)
        .returning(OptionT.some(processingStatus))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe
        json"""{
        "done": ${processingStatus.done.value},
        "total": ${processingStatus.total.value},
        "progress": ${processingStatus.progress.value}
      }"""

      logger.loggedOnly(
        Warn(s"Finding progress status for project '$projectId' finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "return OK the progress status with done = total = 0 if the webhook exists " +
      "but there are no events in the Event Log yet" in new TestCase {

        (hookValidator
          .validateHook(_: Id, _: Option[AccessToken]))
          .expects(projectId, None)
          .returning(context.pure(HookExists))

        (processingStatusFetcher
          .fetchProcessingStatus(_: Id))
          .expects(projectId)
          .returning(OptionT.none[IO, ProcessingStatus])

        val response = fetchProcessingStatus(projectId).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.as[Json].unsafeRunSync() shouldBe
          json"""{
        "done": ${0},
        "total": ${0}
      }"""
      }

    "return NOT_FOUND if the webhook does not exist" in new TestCase {

      (hookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookMissing))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe InfoMessage(
        s"Progress status for project '$projectId' not found"
      ).asJson
    }

    "return NOT_FOUND if no Access Token found" in new TestCase {

      (hookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.raiseError(NoAccessTokenException("error")))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe InfoMessage(
        s"Progress status for project '$projectId' not found"
      ).asJson
    }

    "return INTERNAL_SERVER_ERROR when checking if the webhook exists fails" in new TestCase {

      val exception = exceptions.generateOne
      (hookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.raiseError(exception))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(exception).asJson

      logger.logged(
        Error(s"Finding progress status for project '$projectId' failed", exception)
      )
    }

    "return INTERNAL_SERVER_ERROR when finding progress status fails" in new TestCase {

      (hookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookExists))

      val exception = exceptions.generateOne
      (processingStatusFetcher
        .fetchProcessingStatus(_: Id))
        .expects(projectId)
        .returning(OptionT.liftF(context.raiseError(exception)))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(exception).asJson

      logger.logged(
        Error(s"Finding progress status for project '$projectId' failed", exception)
      )
    }
  }

  private trait TestCase {
    val context = MonadThrow[IO]

    val projectId = projectIds.generateOne

    val hookValidator           = mock[HookValidator[IO]]
    val processingStatusFetcher = mock[ProcessingStatusFetcher[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val fetchProcessingStatus = new ProcessingStatusEndpointImpl[IO](
      hookValidator,
      processingStatusFetcher,
      executionTimeRecorder
    ).fetchProcessingStatus _
  }
}
