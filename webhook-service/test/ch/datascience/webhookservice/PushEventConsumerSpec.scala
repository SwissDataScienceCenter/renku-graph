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

package ch.datascience.webhookservice

import akka.stream.QueueOfferResult.Enqueued
import akka.stream.{Materializer, QueueOfferResult}
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.{CommitId, Project, PushUser}
import ch.datascience.webhookservice.queues.pushevent.{PushEvent, PushEventQueue}
import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.LoggerLike
import play.api.libs.json.{JsError, JsValue, Json}
import play.api.mvc.ControllerComponents
import play.api.test.Helpers._
import play.api.test.{FakeRequest, Injecting}

import scala.concurrent.Future

class PushEventConsumerSpec extends WordSpec with MixedMockFactory with GuiceOneAppPerTest with Injecting {

  "POST /webhook-event" should {

    "return ACCEPTED for valid push event payload which are accepted" in new TestCase {

      val payload: JsValue = Json.obj(
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

      val pushEvent = PushEvent(commitIdBefore, commitIdAfter, pushUser, project)

      (pushEventQueue
        .offer(_: PushEvent))
        .expects(pushEvent)
        .returning(Future.successful(Enqueued))

      val response = call(processPushEvent, request.withBody(payload))

      status(response)          shouldBe ACCEPTED
      contentAsString(response) shouldBe ""

      logger.verify('info)(
        argAssert { (message: () => String) =>
          message() shouldBe s"'$pushEvent' enqueued"
        },
        *
      )
    }

    QueueOfferResult.Dropped +: QueueOfferResult.QueueClosed +: QueueOfferResult.Failure(new Exception("message")) +: Nil foreach {
      queueOfferResult =>
        s"return INTERNAL_SERVER_ERROR for valid push event payload and queue offer result as $queueOfferResult" in new TestCase {
          val payload: JsValue = Json.obj(
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

          val pushEvent = PushEvent(commitIdBefore, commitIdAfter, pushUser, project)

          (pushEventQueue
            .offer(_: PushEvent))
            .expects(pushEvent)
            .returning(Future.successful(queueOfferResult))

          val response = call(processPushEvent, request.withBody(payload))

          status(response)        shouldBe INTERNAL_SERVER_ERROR
          contentAsJson(response) shouldBe ErrorMessage(s"'$pushEvent' enqueueing problem: $queueOfferResult").toJson

          logger.verify('error)(
            argAssert { (message: () => String) =>
              message() shouldBe s"'$pushEvent' enqueueing problem: $queueOfferResult"
            },
            *
          )
        }
    }

    "return BAD_REQUEST for invalid push event payload" in new TestCase {
      val payload: JsValue = Json.obj()

      val response = call(processPushEvent, request.withBody(payload))

      status(response)        shouldBe BAD_REQUEST
      contentAsJson(response) shouldBe a[JsValue]
    }
  }

  private trait TestCase {
    implicit val materializer: Materializer = app.materializer

    val request = FakeRequest().withHeaders(CONTENT_TYPE -> JSON)
    val commitIdBefore: CommitId = commitIds.generateOne
    val commitIdAfter:  CommitId = commitIds.generateOne
    val pushUser:       PushUser = pushUsers.generateOne
    val project:        Project  = projects.generateOne

    val pushEventQueue: PushEventQueue = mock[PushEventQueue]
    val logger           = Proxy.stub[LoggerLike]
    val processPushEvent = new PushEventConsumer(inject[ControllerComponents], logger, pushEventQueue).processPushEvent
  }
}
