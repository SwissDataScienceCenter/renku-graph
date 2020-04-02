/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing.commitevent

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.eventprocessing.CommitEvent
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class CommitEventSenderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "send" should {

    Created +: Ok +: Nil foreach { status =>
      s"succeed when delivering the event to the Event Log got $status" in new TestCase {

        val eventBody = serialize(commitEvent)
        (eventSerializer
          .serialiseToJsonString(_: CommitEvent))
          .expects(commitEvent)
          .returning(context.pure(eventBody))

        stubFor {
          post("/events")
            .withRequestBody(equalToJson(commitEvent.asJson(commitEventEncoder(eventBody)).spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        eventSender.send(commitEvent).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail when delivering the event to the Event Log got $BadRequest" in new TestCase {

      val eventBody = serialize(commitEvent)
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.pure(eventBody))

      val status = BadRequest
      stubFor {
        post("/events")
          .withRequestBody(equalToJson(commitEvent.asJson(commitEventEncoder(eventBody)).spaces2))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        eventSender.send(commitEvent).unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/events returned $status; body: "
    }

    "fail when event serialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventSerializer
        .serialiseToJsonString(_: CommitEvent))
        .expects(commitEvent)
        .returning(context.raiseError(exception))

      intercept[Exception] {
        eventSender.send(commitEvent).unsafeRunSync()
      } shouldBe exception
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val commitEvent = commitEvents.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    class TestCommitEventSerializer extends CommitEventSerializer[IO]
    val eventSerializer = mock[TestCommitEventSerializer]
    val eventSender     = new IOCommitEventSender(eventLogUrl, eventSerializer, TestLogger())
  }

  private def commitEventEncoder(eventBody: String): Encoder[CommitEvent] = Encoder.instance[CommitEvent] { event =>
    json"""{
      "id":        ${event.id.value},
      "project": {
        "id":      ${event.project.id.value},
        "path":    ${event.project.path.value}
      },
      "date":      ${event.committedDate.value},
      "batchDate": ${event.batchDate.value},
      "body":      $eventBody
    }"""
  }
  private def serialize(commitEvent: CommitEvent): String = s"""{id: "${commitEvent.id.toString}"}"""
}
