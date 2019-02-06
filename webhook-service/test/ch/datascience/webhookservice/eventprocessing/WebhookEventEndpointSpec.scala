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

import akka.stream.Materializer
import cats.MonadError
import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.crypto.IOHookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.pushevent.IOPushEventSender
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.model.HookToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import play.api.mvc.ControllerComponents
import play.api.test.Helpers._
import play.api.test.{FakeRequest, Injecting}

class WebhookEventEndpointSpec extends WordSpec with MockFactory with GuiceOneAppPerTest with Injecting {

  "POST /webhooks/events" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {

      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEvent)
        .returning(context.pure(()))

      val request = jsonRequest
        .withBody(pushEventPayloadFrom(pushEvent))
        .withHeaders("X-Gitlab-Token" -> serializedHookToken.toString)

      expectDecryptionOf(serializedHookToken, returning = HookToken(pushEvent.project.id))

      val response = call(processPushEvent, request)

      status(response)          shouldBe ACCEPTED
      contentAsString(response) shouldBe ""
    }

    "return INTERNAL_SERVER_ERROR when storing push event in the event log fails" in new TestCase {

      val exception = exceptions.generateOne
      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEvent)
        .returning(context.raiseError(exception))

      val request = jsonRequest
        .withBody(pushEventPayloadFrom(pushEvent))
        .withHeaders("X-Gitlab-Token" -> serializedHookToken.toString)

      expectDecryptionOf(serializedHookToken, returning = HookToken(pushEvent.project.id))

      val response = call(processPushEvent, request)

      status(response)        shouldBe INTERNAL_SERVER_ERROR
      contentAsJson(response) shouldBe ErrorMessage(exception.getMessage).toJson
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {

      val request = jsonRequest
        .withBody(Json.obj())
        .withHeaders("X-Gitlab-Token" -> serializedHookToken.toString)

      val response = call(processPushEvent, request)

      status(response)        shouldBe BAD_REQUEST
      contentAsJson(response) shouldBe a[JsValue]
    }

    "return UNAUTHORIZED if X-Gitlab-Token token is not present in the header" in new TestCase {

      val request = jsonRequest
        .withBody(pushEventPayloadFrom(pushEvent))

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }

    "return UNAUTHORIZED when user X-Gitlab-Token is invalid" in new TestCase {

      val request = jsonRequest
        .withBody(pushEventPayloadFrom(pushEvent))
        .withHeaders("X-Gitlab-Token" -> serializedHookToken.toString)

      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(context.pure(HookToken(projectIds.generateOne)))

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }

    "return UNAUTHORIZED when X-Gitlab-Token decryption fails" in new TestCase {

      val request = jsonRequest
        .withBody(pushEventPayloadFrom(pushEvent))
        .withHeaders("X-Gitlab-Token" -> serializedHookToken.toString)

      val exception = new Exception("decryption failure")
      (hookTokenCrypto
        .decrypt(_: SerializedHookToken))
        .expects(serializedHookToken)
        .returning(context.raiseError(exception))

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }
  }

  private trait TestCase {
    implicit val materializer: Materializer = app.materializer
    val context = MonadError[IO, Throwable]

    val jsonRequest = FakeRequest().withHeaders(CONTENT_TYPE -> JSON)
    val pushEvent   = pushEvents.generateOne
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
    val processPushEvent = new WebhookEventEndpoint(
      inject[ControllerComponents],
      hookTokenCrypto,
      pushEventSender
    ).processPushEvent

    def pushEventPayloadFrom(pushEvent: PushEvent): JsObject =
      Json.obj(
        Seq(
          pushEvent.maybeCommitFrom.map(before => "before" -> toJsFieldJsValueWrapper(before.value)),
          Some("after"         -> toJsFieldJsValueWrapper(pushEvent.commitTo.value)),
          Some("user_id"       -> toJsFieldJsValueWrapper(pushEvent.pushUser.userId.value)),
          Some("user_username" -> toJsFieldJsValueWrapper(pushEvent.pushUser.username.value)),
          pushEvent.pushUser.maybeEmail.map(email => "user_email" -> toJsFieldJsValueWrapper(email.value)),
          Some(
            "project" -> toJsFieldJsValueWrapper(
              Json.obj(
                "id"                  -> toJsFieldJsValueWrapper(pushEvent.project.id.value),
                "path_with_namespace" -> toJsFieldJsValueWrapper(pushEvent.project.path.value)
              )
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
