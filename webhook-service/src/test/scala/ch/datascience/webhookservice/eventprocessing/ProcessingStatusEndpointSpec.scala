/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing

import cats.MonadError
import cats.data.OptionT
import cats.effect.IO
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.dbeventlog.DbEventLogGenerators.processingStatuses
import ch.datascience.dbeventlog.commands.{IOEventLogProcessingStatus, ProcessingStatus}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.EventsGenerators.projectIds
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.AccessToken
import ch.datascience.http.server.EndpointTester._
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
<<<<<<< HEAD
=======
import ch.datascience.webhookservice.hookvalidation.HookValidator.NoAccessTokenException
>>>>>>> 2af18b6... fix: status endpoint to return NOT_FOUND when no hook for a project
import ch.datascience.webhookservice.hookvalidation.IOHookValidator
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ProcessingStatusEndpointSpec extends WordSpec with MockFactory {

  "fetchProcessingStatus" should {

    "return OK with the progress status for the given projectId if the webhook exists" in new TestCase {

      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookExists))

      val processingStatus = processingStatuses.generateOne
      (eventsProcessingStatus
        .fetchStatus(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.some(processingStatus))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe Ok
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe json"""{
        "done": ${processingStatus.done.value},
        "total": ${processingStatus.total.value},
        "progress": ${processingStatus.progress.value}
      }"""
    }

    "return NOT_FOUND if no progress status can be found for the projectId " +
      "even if the webhook exists" in new TestCase {

      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookExists))

      (eventsProcessingStatus
        .fetchStatus(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.none[IO, ProcessingStatus])

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe NotFound
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"Progress status for project '$projectId' not found").asJson
    }

    "return NOT_FOUND if the webhook does not exist" in new TestCase {

      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookMissing))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe NotFound
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"Progress status for project '$projectId' not found").asJson
    }

    "return NOT_FOUND if no Access Token found" in new TestCase {

      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.raiseError(NoAccessTokenException("error")))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe NotFound
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"Progress status for project '$projectId' not found").asJson
    }

    "return INTERNAL_SERVER_ERROR when checking if the webhook exists fails" in new TestCase {

      val exception = exceptions.generateOne
      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.raiseError(exception))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe InternalServerError
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage(exception.getMessage).asJson
    }

    "return NOT_FOUND if the webhook does not exist" in new TestCase {

      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookMissing))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe NotFound
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"Project: $projectId not found").asJson
    }

    "return INTERNAL_SERVER_ERROR when checking if the webhook exists fails" in new TestCase {

      val exception = exceptions.generateOne
      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.raiseError(exception))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe InternalServerError
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage(exception.getMessage).asJson
    }

    "return INTERNAL_SERVER_ERROR when finding progress status fails" in new TestCase {

      (hookValidator
        .validateHook(_: ProjectId, _: Option[AccessToken]))
        .expects(projectId, None)
        .returning(context.pure(HookExists))

      val exception = exceptions.generateOne
      (eventsProcessingStatus
        .fetchStatus(_: ProjectId))
        .expects(projectId)
        .returning(OptionT.liftF(context.raiseError(exception)))

      val response = fetchProcessingStatus(projectId).unsafeRunSync()

      response.status                 shouldBe InternalServerError
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage(exception.getMessage).asJson
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val projectId = projectIds.generateOne

    val hookValidator          = mock[IOHookValidator]
    val eventsProcessingStatus = mock[IOEventLogProcessingStatus]
    val fetchProcessingStatus = new ProcessingStatusEndpoint[IO](
      hookValidator,
      eventsProcessingStatus
    ).fetchProcessingStatus _
  }
}
