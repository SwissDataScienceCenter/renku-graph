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
import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.crypto.IOHookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.pushevent.IOPushEventSender
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import ch.datascience.webhookservice.model.HookToken
import io.circe.Json
import io.circe.syntax._
import org.http4s.Status._
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class HookEventEndpointSpec extends WordSpec with MockFactory {

  "POST /webhooks/events" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {

      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEvent)
        .returning(context.pure(()))

      expectDecryptionOf(serializedHookToken, returning = HookToken(pushEvent.project.id))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("webhooks") / "events")
          .withHeaders(Headers(Header("X-Gitlab-Token", serializedHookToken.toString)))
          .withEntity(pushEventPayloadFrom(pushEvent))
      )

      response.status       shouldBe Accepted
      response.body[String] shouldBe ""
    }

    "return INTERNAL_SERVER_ERROR when storing push event in the event log fails" in new TestCase {

      val exception = exceptions.generateOne
      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEvent)
        .returning(context.raiseError(exception))

      expectDecryptionOf(serializedHookToken, returning = HookToken(pushEvent.project.id))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("webhooks") / "events")
          .withHeaders(Headers(Header("X-Gitlab-Token", serializedHookToken.toString)))
          .withEntity(pushEventPayloadFrom(pushEvent))
      )

      response.status     shouldBe InternalServerError
      response.body[Json] shouldBe ErrorMessage(exception.getMessage).asJson
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("webhooks") / "events")
          .withHeaders(Headers(Header("X-Gitlab-Token", serializedHookToken.toString)))
          .withEntity(Json.obj())
      )

      response.status     shouldBe BadRequest
      response.body[Json] shouldBe ErrorMessage("Invalid message body: Could not decode JSON: {}").asJson
    }

    "return UNAUTHORIZED if X-Gitlab-Token token is not present in the header" in new TestCase {

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("webhooks") / "events")
          .withEntity(pushEventPayloadFrom(pushEvent))
      )

      response.status     shouldBe Unauthorized
      response.body[Json] shouldBe ErrorMessage(UnauthorizedException.getMessage).asJson
    }

    "return UNAUTHORIZED when user X-Gitlab-Token is invalid" in new TestCase {

      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(context.pure(HookToken(projectIds.generateOne)))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("webhooks") / "events")
          .withHeaders(Headers(Header("X-Gitlab-Token", serializedHookToken.toString)))
          .withEntity(pushEventPayloadFrom(pushEvent))
      )

      response.status     shouldBe Unauthorized
      response.body[Json] shouldBe ErrorMessage(UnauthorizedException.getMessage).asJson
    }

    "return UNAUTHORIZED when X-Gitlab-Token decryption fails" in new TestCase {

      val exception = new Exception("decryption failure")
      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(context.raiseError(exception))

      val response = endpoint.call(
        Request(Method.POST, Uri.uri("webhooks") / "events")
          .withHeaders(Headers(Header("X-Gitlab-Token", serializedHookToken.toString)))
          .withEntity(pushEventPayloadFrom(pushEvent))
      )

      response.status     shouldBe Unauthorized
      response.body[Json] shouldBe ErrorMessage(UnauthorizedException.getMessage).asJson
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val pushEvent = pushEvents.generateOne
    val serializedHookToken = nonEmptyStrings().map {
      SerializedHookToken
        .from(_)
        .fold(
          exception => throw exception,
          identity
        )
    }.generateOne

    val pushEventSender = mock[IOPushEventSender]
    val hookTokenCrypto = mock[IOHookTokenCrypto]
    val endpoint = new HookEventEndpoint[IO](
      hookTokenCrypto,
      pushEventSender
    ).processPushEvent.or(notAvailableResponse)

    def pushEventPayloadFrom(pushEvent: PushEvent) =
      Json.obj(
        Seq(
          pushEvent.maybeCommitFrom.map(before => "before" -> Json.fromString(before.value)),
          Some("after"         -> Json.fromString(pushEvent.commitTo.value)),
          Some("user_id"       -> Json.fromInt(pushEvent.pushUser.userId.value)),
          Some("user_username" -> Json.fromString(pushEvent.pushUser.username.value)),
          pushEvent.pushUser.maybeEmail.map(email => "user_email" -> Json.fromString(email.value)),
          Some(
            "project" -> Json.obj(
              "id"                  -> Json.fromInt(pushEvent.project.id.value),
              "path_with_namespace" -> Json.fromString(pushEvent.project.path.value)
            )
          )
        ).flatten: _*
      )

    def expectDecryptionOf(hookAuthToken: SerializedHookToken, returning: HookToken) =
      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(hookAuthToken)
        .returning(IO.pure(returning))
  }
}
