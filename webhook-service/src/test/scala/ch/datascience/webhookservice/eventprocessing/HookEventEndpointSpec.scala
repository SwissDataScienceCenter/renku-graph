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

package ch.datascience.webhookservice.eventprocessing

import cats.MonadError
import cats.effect.IO
import ch.datascience.http.ErrorMessage._
import ch.datascience.http.InfoMessage
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.crypto.IOHookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.startcommit.IOCommitToEventLog
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.model.HookToken
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookEventEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "processPushEvent" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {

      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(startCommit)
        .returning(context.pure(()))

      expectDecryptionOf(serializedHookToken, returning = HookToken(startCommit.project.id))

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(startCommit))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Accepted
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe InfoMessage("Event accepted").asJson
    }

    "return INTERNAL_SERVER_ERROR when storing push event in the event log fails and log the error" in new TestCase {

      val exception = exceptions.generateOne
      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(startCommit)
        .returning(context.raiseError(exception))

      expectDecryptionOf(serializedHookToken, returning = HookToken(startCommit.project.id))

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(startCommit))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(exception).asJson

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(Json.obj())

      val response = processPushEvent(request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(
        s"Invalid message body: Could not decode JSON: ${Json.obj()}"
      ).asJson
    }

    "return UNAUTHORIZED if X-Gitlab-Token token is not present in the header" in new TestCase {

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withEntity(pushEventPayloadFrom(startCommit))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }

    "return UNAUTHORIZED when user X-Gitlab-Token is invalid" in new TestCase {

      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(context.pure(HookToken(projectIds.generateOne)))

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(startCommit))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }

    "return UNAUTHORIZED when X-Gitlab-Token decryption fails" in new TestCase {

      val exception = new Exception("decryption failure")
      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(context.raiseError(exception))

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(startCommit))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }
  }

  "PushEvent deserialization" should {

    "work if 'before' is null" in new TestCase {
      import HookEventEndpoint.pushEventDecoder

      pushEventPayloadFrom(startCommit).as[StartCommit] shouldBe Right(startCommit)
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val logger = TestLogger[IO]()

    val startCommit = startCommits.generateOne
    val serializedHookToken = nonEmptyStrings().map {
      SerializedHookToken
        .from(_)
        .fold(
          exception => throw exception,
          identity
        )
    }.generateOne

    val commitToEventLog = mock[IOCommitToEventLog]
    val hookTokenCrypto  = mock[IOHookTokenCrypto]
    val processPushEvent = new HookEventEndpoint[IO](
      hookTokenCrypto,
      commitToEventLog,
      logger
    ).processPushEvent _

    def pushEventPayloadFrom(commit: StartCommit) = json"""
      {                                                      
        "after": ${commit.id.value},
        "project": {
          "id":                  ${commit.project.id.value},
          "path_with_namespace": ${commit.project.path.value}
        }
      }
    """

    def expectDecryptionOf(hookAuthToken: SerializedHookToken, returning: HookToken) =
      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(hookAuthToken)
        .returning(IO.pure(returning))
  }
}
