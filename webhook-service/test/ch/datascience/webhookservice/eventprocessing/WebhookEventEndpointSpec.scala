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
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.crypto.HookTokenCrypto.HookAuthToken
import ch.datascience.webhookservice.crypto.IOHookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.pushevent.IOPushEventSender
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import play.api.mvc.ControllerComponents
import play.api.test.Helpers._
import play.api.test.{FakeRequest, Injecting}

class WebhookEventEndpointSpec extends WordSpec with MockFactory with GuiceOneAppPerTest with Injecting {

  "POST /webhook-event" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {

      (pushEventSender
        .storeCommitsInEventLog(_: PushEvent))
        .expects(pushEvent)
        .returning(context.pure(()))

      val hookToken = hookTokenFor(pushEvent)
      val request = jsonRequest
        .withBody(payloadFor(pushEvent))
        .withHeaders("X-Gitlab-Token" -> hookToken.toString())

      expectDecryptionOf(hookToken, returning = pushEvent.project.id)

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

      val hookToken = hookTokenFor(pushEvent)
      val request = jsonRequest
        .withBody(payloadFor(pushEvent))
        .withHeaders("X-Gitlab-Token" -> hookToken.toString())

      expectDecryptionOf(hookToken, returning = pushEvent.project.id)

      val response = call(processPushEvent, request)

      status(response)        shouldBe INTERNAL_SERVER_ERROR
      contentAsJson(response) shouldBe ErrorMessage(exception.getMessage).toJson
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {

      val hookToken = hookTokenFor(pushEvent)
      val request = jsonRequest
        .withBody(Json.obj())
        .withHeaders("X-Gitlab-Token" -> hookToken.toString())

      val response = call(processPushEvent, request)

      status(response)        shouldBe BAD_REQUEST
      contentAsJson(response) shouldBe a[JsValue]
    }

    "return UNAUTHORIZED if X-Gitlab-Token token is not present in the header" in new TestCase {

      val request = jsonRequest
        .withBody(payloadFor(pushEvent))

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }

    "return UNAUTHORIZED when user X-Gitlab-Token is invalid" in new TestCase {

      val otherProjectId   = projectIds.generateOne
      val invalidHookToken = hookTokenFor(otherProjectId)
      val request = jsonRequest
        .withBody(payloadFor(pushEvent))
        .withHeaders("X-Gitlab-Token" -> invalidHookToken.toString())

      (hookTokenCrypto
        .decrypt(_: HookAuthToken))
        .expects(invalidHookToken)
        .returning(context.pure(otherProjectId.toString()))

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }

    "return UNAUTHORIZED when X-Gitlab-Token decryption fails" in new TestCase {

      val invalidHookToken = hookTokenFor(projectIds.generateOne)

      val request = jsonRequest
        .withBody(payloadFor(pushEvent))
        .withHeaders("X-Gitlab-Token" -> invalidHookToken.toString())

      val exception = new Exception("decryption failure")
      (hookTokenCrypto
        .decrypt(_: HookAuthToken))
        .expects(invalidHookToken)
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
    val pushEvent: PushEvent = pushEvents.generateOne

    val pushEventSender = mock[IOPushEventSender]
    val hookTokenCrypto = mock[IOHookTokenCrypto]
    val processPushEvent = new WebhookEventEndpoint(
      inject[ControllerComponents],
      hookTokenCrypto,
      pushEventSender
    ).processPushEvent

    def payloadFor(pushEvent: PushEvent): JsObject =
      pushEvent.maybeBefore.foldLeft(commonJson(pushEvent)) { (json, before) =>
        json + ("before" -> JsString(before.value))
      }

    private def commonJson(pushEvent: PushEvent) = Json.obj(
      "after"         -> pushEvent.after.value,
      "user_id"       -> pushEvent.pushUser.userId.value,
      "user_username" -> pushEvent.pushUser.username.value,
      "user_email"    -> pushEvent.pushUser.email.value,
      "project" -> Json.obj(
        "id"                  -> pushEvent.project.id.value,
        "path_with_namespace" -> pushEvent.project.path.value
      )
    )

    def hookTokenFor(pushEvent: PushEvent): HookAuthToken =
      hookTokenFor(pushEvent.project.id)

    def hookTokenFor(projectId: ProjectId): HookAuthToken =
      HookAuthToken
        .from(projectId.toString)
        .fold(
          exception => throw exception,
          identity
        )

    def expectDecryptionOf(hookAuthToken: HookAuthToken, returning: ProjectId) =
      (hookTokenCrypto
        .decrypt(_: HookAuthToken))
        .expects(hookAuthToken)
        .returning(IO.pure(returning.toString()))
  }
}
