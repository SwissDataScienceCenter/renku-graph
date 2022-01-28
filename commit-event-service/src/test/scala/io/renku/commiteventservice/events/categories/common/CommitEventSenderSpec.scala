/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.categories.common

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.commiteventservice.events.categories.common.CommitEvent._
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.config.EventLogUrl
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommitEventSenderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "send" should {

    s"return successfully when delivering the event to the Event Log got $Accepted" in new TestCase {

      val eventBody = serialize(newCommitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(newCommitEvent)
        .returning(eventBody.pure[IO])

      stubFor {
        post("/events")
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(newCommitEvent.asJson(commitEventEncoder(eventBody)).spaces2))
          )
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      eventSender.send(newCommitEvent).unsafeRunSync() shouldBe ()
    }

    s"return successfully for a SkippedCommitEvent" in new TestCase {
      val skippedCommitEvent = skippedCommitEvents.generateOne
      val eventBody          = serialize(newCommitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(skippedCommitEvent)
        .returning(eventBody.pure[IO])

      stubFor {
        post("/events")
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(skippedCommitEvent.asJson(commitEventEncoder(eventBody)).spaces2))
          )
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      eventSender.send(skippedCommitEvent).unsafeRunSync() shouldBe ()
    }

    s"fail when delivering the event to the Event Log got $BadRequest" in new TestCase {

      val eventBody = serialize(newCommitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(newCommitEvent)
        .returning(eventBody.pure[IO])

      val status = BadRequest
      stubFor {
        post("/events")
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(newCommitEvent.asJson(commitEventEncoder(eventBody)).spaces2))
          )
          .willReturn(aResponse().withStatus(BadRequest.code))
      }

      intercept[Exception] {
        eventSender.send(newCommitEvent).unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/events returned $status; body: "
    }

    "fail when event serialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(newCommitEvent)
        .returning(exception.raiseError[IO, String])

      intercept[Exception] {
        eventSender.send(newCommitEvent).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val newCommitEvent = newCommitEvents.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger()
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    class TestCommitEventSerializer extends CommitEventSerializer[IO]
    val eventSerializer = mock[TestCommitEventSerializer]
    val eventSender     = new CommitEventSenderImpl[IO](eventLogUrl, eventSerializer)
  }

  private def commitEventEncoder[T <: CommitEvent](eventBody: String): Encoder[T] = Encoder.instance[T] {
    case event: NewCommitEvent =>
      json"""{
      "categoryName": "CREATION",
      "id":        ${event.id.value},
      "project": {
        "id":      ${event.project.id.value},
        "path":    ${event.project.path.value}
      },
      "date":      ${event.committedDate.value},
      "batchDate": ${event.batchDate.value},
      "body":      $eventBody,
      "status":    ${event.status.value}
    }"""
    case event: SkippedCommitEvent =>
      json"""{
      "categoryName": "CREATION",
      "id":        ${event.id.value},
      "project": {
        "id":      ${event.project.id.value},
        "path":    ${event.project.path.value}
      },
      "date":      ${event.committedDate.value},
      "batchDate": ${event.batchDate.value},
      "body":      $eventBody,
      "status":    ${event.status.value},
      "message":   ${event.message.value}
    }"""
  }
  private def serialize(commitEvent: CommitEvent): String = s"""{id: "${commitEvent.id.toString}"}"""
}
