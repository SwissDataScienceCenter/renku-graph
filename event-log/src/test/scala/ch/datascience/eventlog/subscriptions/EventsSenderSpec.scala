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

package ch.datascience.eventlog.subscriptions

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.eventlog.DbEventLogGenerators._
import ch.datascience.eventlog.Event
import ch.datascience.eventlog.subscriptions.EventsSender.SendingResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventsSenderSpec extends WordSpec with ExternalServiceStubbing {

  "sendEvent" should {

    s"return Delivered if remote responds with $Accepted" in new TestCase {
      stubFor {
        post("/")
          .withRequestBody(equalToJson(event.asJson.spaces2))
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.sendEvent(subscriptionUrl, event.compoundEventId, event.body).unsafeRunSync() shouldBe Delivered
    }

    s"return ServiceBusy if remote responds with $TooManyRequests" in new TestCase {
      stubFor {
        post("/")
          .withRequestBody(equalToJson(event.asJson.spaces2))
          .willReturn(aResponse().withStatus(TooManyRequests.code))
      }

      sender.sendEvent(subscriptionUrl, event.compoundEventId, event.body).unsafeRunSync() shouldBe ServiceBusy
    }

    NotFound +: BadGateway +: ServiceUnavailable +: Nil foreach { status =>
      s"return Misdelivered if remote responds with $status" in new TestCase {
        stubFor {
          post("/")
            .withRequestBody(equalToJson(event.asJson.spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        sender.sendEvent(subscriptionUrl, event.compoundEventId, event.body).unsafeRunSync() shouldBe Misdelivered
      }
    }

    "return Misdelivered if call to the remote fails with Connect Exception" in new TestCase {
      override val sender = new IOEventsSender(TestLogger())

      sender
        .sendEvent(SubscriptionUrl("http://unexisting"), event.compoundEventId, event.body)
        .unsafeRunSync() shouldBe Misdelivered
    }

    s"fail if remote responds with $BadRequest" in new TestCase {
      stubFor {
        post("/")
          .withRequestBody(equalToJson(event.asJson.spaces2))
          .willReturn(badRequest().withBody("message"))
      }

      intercept[Exception] {
        sender.sendEvent(subscriptionUrl, event.compoundEventId, event.body).unsafeRunSync()
      }.getMessage shouldBe s"POST $subscriptionUrl returned $BadRequest; body: message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val event           = events.generateOne
    val subscriptionUrl = SubscriptionUrl(externalServiceBaseUrl)

    val sender = new IOEventsSender(TestLogger())
  }

  private implicit val eventEncoder: Encoder[Event] = Encoder.instance[Event] { event =>
    json"""{
      "id":      ${event.id.value},
      "project": {
        "id":    ${event.project.id.value}
      },
      "body":    ${event.body.value}
    }"""
  }
}
