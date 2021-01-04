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

package io.renku.eventlog.subscriptions

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{eventBodies, eventIds}
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventId}
import ch.datascience.graph.model.{events, projects}
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.CompoundId
import io.renku.eventlog.subscriptions.EventsSender.SendingResult.{Delivered, Misdelivered, ServiceBusy}
import org.http4s.Status._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsSenderSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "sendEvent" should {

    s"return Delivered if remote responds with $Accepted" in new TestCase {
      stubFor {
        post("/")
          .withRequestBody(equalToJson(event.asJson.spaces2))
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.sendEvent(subscriberUrl, event.compoundEventId, event.body).unsafeRunSync() shouldBe Delivered
    }

    TooManyRequests +: ServiceUnavailable +: Nil foreach { status =>
      s"return ServiceBusy if remote responds with $status" in new TestCase {
        stubFor {
          post("/")
            .withRequestBody(equalToJson(event.asJson.spaces2))
            .willReturn(aResponse().withStatus(TooManyRequests.code))
        }

        sender.sendEvent(subscriberUrl, event.compoundEventId, event.body).unsafeRunSync() shouldBe ServiceBusy
      }
    }

    NotFound +: BadGateway +: Nil foreach { status =>
      s"return Misdelivered if remote responds with $status" in new TestCase {
        stubFor {
          post("/")
            .withRequestBody(equalToJson(event.asJson.spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        sender.sendEvent(subscriberUrl, event.compoundEventId, event.body).unsafeRunSync() shouldBe Misdelivered
      }
    }

    "return Misdelivered if call to the remote fails with Connect Exception" in new TestCase {
      override val sender = new IOEventsSender(TestLogger(), retryInterval = 10 millis)

      sender
        .sendEvent(SubscriberUrl("http://unexisting"), event.compoundEventId, event.body)
        .unsafeRunSync() shouldBe Misdelivered
    }

    s"fail if remote responds with $BadRequest" in new TestCase {
      stubFor {
        post("/")
          .withRequestBody(equalToJson(event.asJson.spaces2))
          .willReturn(badRequest().withBody("message"))
      }

      intercept[Exception] {
        sender.sendEvent(subscriberUrl, event.compoundEventId, event.body).unsafeRunSync()
      }.getMessage shouldBe s"POST $subscriberUrl returned $BadRequest; body: message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val event         = readyEvents.generateOne
    val subscriberUrl = SubscriberUrl(externalServiceBaseUrl)

    val sender = new IOEventsSender(TestLogger())
  }

  private implicit val eventEncoder: Encoder[ReadyEvent] = Encoder.instance[ReadyEvent] { event =>
    json"""{
      "id":      ${event.id.value},
      "project": {
        "id":    ${event.projectId.value}
      },
      "body":    ${event.body.value}
    }"""
  }

  private case class ReadyEvent(id: EventId, projectId: projects.Id, body: EventBody) extends CompoundId {
    override lazy val compoundEventId: events.CompoundEventId = CompoundEventId(id, projectId)
  }

  private implicit val readyEvents: Gen[ReadyEvent] = for {
    eventId   <- eventIds
    projectId <- projectIds
    body      <- eventBodies
  } yield ReadyEvent(eventId, projectId, body)

}
