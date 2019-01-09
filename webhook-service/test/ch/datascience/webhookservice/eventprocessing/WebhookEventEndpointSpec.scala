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

import akka.stream.QueueOfferResult.Enqueued
import akka.stream.{Materializer, QueueOfferResult}
import cats.MonadError
import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.{CommitId, Project, ProjectId, PushUser}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.crypto.HookTokenCrypto.{HookAuthToken, Secret}
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.queues.pushevent.{PushEvent, PushEventQueue}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.mvc.ControllerComponents
import play.api.test.Helpers._
import play.api.test.{FakeRequest, Injecting}

import scala.concurrent.Future

class WebhookEventEndpointSpec extends WordSpec with MockFactory with GuiceOneAppPerTest with Injecting {

  "POST /webhook-event" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {

      val pushEvent = PushEvent(commitIdBefore, commitIdAfter, pushUser, project)

      (pushEventQueue
        .offer(_: PushEvent))
        .expects(pushEvent)
        .returning(Future.successful(Enqueued))

      val request = jsonRequest
        .withBody(newValidPayload)
        .withHeaders("X-Gitlab-Token" -> validHookToken.toString())

      expectDecryptionOf(validHookToken, returning = project.id)

      val response = call(processPushEvent, request)

      status(response)          shouldBe ACCEPTED
      contentAsString(response) shouldBe ""

      logger.loggedOnly(Info, s"'$pushEvent' enqueued")
    }

    QueueOfferResult.Dropped +: QueueOfferResult.QueueClosed +: QueueOfferResult.Failure(new Exception("message")) +: Nil foreach {
      queueOfferResult =>
        s"return INTERNAL_SERVER_ERROR for valid push event payload and queue offer result as $queueOfferResult" in new TestCase {

          val pushEvent = PushEvent(commitIdBefore, commitIdAfter, pushUser, project)

          (pushEventQueue
            .offer(_: PushEvent))
            .expects(pushEvent)
            .returning(Future.successful(queueOfferResult))

          val request = jsonRequest
            .withBody(newValidPayload)
            .withHeaders("X-Gitlab-Token" -> validHookToken.toString())

          expectDecryptionOf(validHookToken, returning = project.id)

          val response = call(processPushEvent, request)

          status(response)        shouldBe INTERNAL_SERVER_ERROR
          contentAsJson(response) shouldBe ErrorMessage(s"'$pushEvent' enqueueing problem: $queueOfferResult").toJson

          logger.loggedOnly(Error, s"'$pushEvent' enqueueing problem: $queueOfferResult")
        }
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {

      val request = jsonRequest
        .withBody(Json.obj())
        .withHeaders("X-Gitlab-Token" -> validHookToken.toString())

      val response = call(processPushEvent, request)

      status(response)        shouldBe BAD_REQUEST
      contentAsJson(response) shouldBe a[JsValue]
    }

    "return UNAUTHORIZED if X-Gitlab-Token token is not present in the header" in new TestCase {

      val request = jsonRequest
        .withBody(newValidPayload)

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
      logger.loggedOnly(Error, UnauthorizedException.getMessage)
    }

    "return UNAUTHORIZED when user X-Gitlab-Token is invalid" in new TestCase {

      val otherProjectId   = projectIds.generateOne
      val invalidHookToken = hookTokenFor(otherProjectId)
      val request = jsonRequest
        .withBody(newValidPayload)
        .withHeaders("X-Gitlab-Token" -> invalidHookToken.toString())

      (hookTokenCrypto
        .decrypt(_: HookAuthToken))
        .expects(invalidHookToken)
        .returning(IO.pure(otherProjectId.toString()))

      val response = call(processPushEvent, request)

      status(response)        shouldBe UNAUTHORIZED
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
      logger.loggedOnly(Error, UnauthorizedException.getMessage)
    }

    "return INTERNAL_SERVER_ERROR when X-Gitlab-Token decryption fails" in new TestCase {

      val invalidHookToken = hookTokenFor(projectIds.generateOne)

      val request = jsonRequest
        .withBody(newValidPayload)
        .withHeaders("X-Gitlab-Token" -> invalidHookToken.toString())

      val exception = new Exception("decryption failure")
      (hookTokenCrypto
        .decrypt(_: HookAuthToken))
        .expects(invalidHookToken)
        .returning(IO.raiseError(exception))

      val response = call(processPushEvent, request)

      status(response)        shouldBe INTERNAL_SERVER_ERROR
      contentAsJson(response) shouldBe ErrorMessage(exception.getMessage).toJson
      logger.loggedOnly(Error, exception.getMessage)
    }
  }

  private trait TestCase {
    implicit val materializer: Materializer = app.materializer

    val jsonRequest = FakeRequest().withHeaders(CONTENT_TYPE -> JSON)
    val commitIdBefore: CommitId = commitIds.generateOne
    val commitIdAfter:  CommitId = commitIds.generateOne
    val pushUser:       PushUser = pushUsers.generateOne
    val project:        Project  = projects.generateOne

    val pushEventQueue: PushEventQueue = mock[PushEventQueue]
    val logger = TestLogger[IO]()
    class MockTokenCrypto(secret: Secret)(implicit ME: MonadError[IO, Throwable])
        extends HookTokenCrypto[IO](secret)(ME)
    val hookTokenCrypto = mock[MockTokenCrypto]
    val processPushEvent = new WebhookEventEndpoint(
      inject[ControllerComponents],
      logger,
      hookTokenCrypto,
      pushEventQueue
    ).processPushEvent

    def newValidPayload: JsObject = Json.obj(
      "before"        -> commitIdBefore.value,
      "after"         -> commitIdAfter.value,
      "user_id"       -> pushUser.userId.value,
      "user_username" -> pushUser.username.value,
      "user_email"    -> pushUser.email.value,
      "project" -> Json.obj(
        "id"                  -> project.id.value,
        "path_with_namespace" -> project.path.value
      )
    )

    lazy val validHookToken: HookAuthToken = hookTokenFor(project.id)

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
