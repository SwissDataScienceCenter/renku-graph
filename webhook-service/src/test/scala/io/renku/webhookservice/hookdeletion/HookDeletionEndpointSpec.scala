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

package io.renku.webhookservice.hookdeletion

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax.EncoderOps
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.http.client.AccessToken
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators.projectHookIds
import io.renku.webhookservice.hookdeletion.HookRemover.DeletionResult.{HookDeleted, HookNotFound}
import io.renku.webhookservice.model.HookIdentifier
import org.http4s.MediaType
import org.http4s.Status.{InternalServerError, NotFound, Ok, Unauthorized}
import org.http4s.headers.`Content-Type`
import org.http4s.circe.CirceEntityCodec._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookDeletionEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "deleteHook" should {

    "return OK when webhook is successfully deleted for project with the given id in GitLab" in new TestCase {

      (hookRemover
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(HookDeleted.some.pure[IO])

      val response = endpoint.deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                      shouldBe Ok
      response.contentType                 shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Info("Hook deleted")
    }

    HookNotFound.some :: None :: Nil foreach { deletionResult =>
      s"return NOT_FOUND when hook deletion returned $deletionResult" in new TestCase {

        (hookRemover
          .deleteHook(_: HookIdentifier, _: AccessToken))
          .expects(projectHookId, authUser.accessToken)
          .returning(HookNotFound.some.pure[IO])

        val response = endpoint.deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

        response.status                      shouldBe NotFound
        response.contentType                 shouldBe Some(`Content-Type`(MediaType.application.json))
        response.as[Message].unsafeRunSync() shouldBe Message.Info("Hook not found")
      }
    }

    "return INTERNAL_SERVER_ERROR when there was an error during hook deletion and log the error" in new TestCase {

      val errorMessage = Message.Error("some error")
      val exception    = new Exception(errorMessage.show)
      (hookRemover
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(IO.raiseError(exception))

      val response = endpoint.deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe errorMessage.asJson

      logger.loggedOnly(Error(exception.getMessage, exception))
    }

    "return UNAUTHORIZED when there was an UnauthorizedException thrown during hook creation" in new TestCase {

      (hookRemover
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(IO.raiseError(UnauthorizedException))

      val response = endpoint.deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                      shouldBe Unauthorized
      response.contentType                 shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Error.fromExceptionMessage(UnauthorizedException)
    }
  }

  private trait TestCase {

    val projectHookId = projectHookIds.generateOne
    val authUser      = authUsers.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val hookRemover = mock[HookRemover[IO]]

    val endpoint = new HookDeletionEndpointImpl[IO](projectHookId.projectHookUrl, hookRemover)
  }
}
