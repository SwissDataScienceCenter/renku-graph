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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.ErrorMessage._
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.webhookservice.CommitSyncRequestSender
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.crypto.IOHookTokenCrypto
import io.renku.webhookservice.model.{CommitSyncRequest, HookToken}
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

      (commitSyncRequestSender
        .sendCommitSyncRequest(_: CommitSyncRequest))
        .expects(syncRequest)
        .returning(().pure[IO])

      expectDecryptionOf(serializedHookToken, returning = HookToken(syncRequest.project.id))

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(commitId, syncRequest))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Accepted
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe InfoMessage("Event accepted").asJson

      logger.loggedOnly(
        Info(
          s"Push event for eventId = $commitId, projectId = ${syncRequest.project.id}, projectPath = ${syncRequest.project.path} -> accepted"
        )
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
        .withEntity(pushEventPayloadFrom(commitId, syncRequest))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }

    "return UNAUTHORIZED when user X-Gitlab-Token is invalid" in new TestCase {

      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(HookToken(projectIds.generateOne).pure[IO])

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(commitId, syncRequest))

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
        .returning(exception.raiseError[IO, HookToken])

      val request = Request(Method.POST, uri"webhooks" / "events")
        .withHeaders(Headers.of(Header("X-Gitlab-Token", serializedHookToken.toString)))
        .withEntity(pushEventPayloadFrom(commitId, syncRequest))

      val response = processPushEvent(request).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }
  }

  private trait TestCase {

    val commitId    = commitIds.generateOne
    val syncRequest = commitSyncRequests.generateOne
    val serializedHookToken = nonEmptyStrings().map {
      SerializedHookToken
        .from(_)
        .fold(
          exception => throw exception,
          identity
        )
    }.generateOne

    val logger                  = TestLogger[IO]()
    val commitSyncRequestSender = mock[CommitSyncRequestSender[IO]]
    val hookTokenCrypto         = mock[IOHookTokenCrypto]
    val processPushEvent = new HookEventEndpointImpl[IO](
      hookTokenCrypto,
      commitSyncRequestSender,
      logger
    ).processPushEvent _

    def expectDecryptionOf(hookAuthToken: SerializedHookToken, returning: HookToken) =
      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(hookAuthToken)
        .returning(IO.pure(returning))
  }

  private def pushEventPayloadFrom(commitId: CommitId, syncRequest: CommitSyncRequest) =
    json"""{                                                      
      "after":                 ${commitId.value},
      "project": {
        "id":                  ${syncRequest.project.id.value},
        "path_with_namespace": ${syncRequest.project.path.value}
      }
    }"""
}
