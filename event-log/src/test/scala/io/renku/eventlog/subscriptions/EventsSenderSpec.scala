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
import cats.implicits.catsSyntaxOptionId
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.eventlog.subscriptions.EventsSender.SendingResult.{Delivered, Misdelivered, ServiceBusy}
import io.renku.eventlog.subscriptions.TestCategoryEvent._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsSenderSpec extends AnyWordSpec with ExternalServiceStubbing with MockFactory with should.Matchers {

  "sendEvent" should {

    s"return Delivered if remote responds with $Accepted" in new TestCase {

      val (eventJson, eventPayload) = expectEventEncoding(event)
      stubFor {
        post("/")
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(eventJson.spaces2))
          )
          .withMultipartRequestBody(
            aMultipart("payload")
              .withBody(equalTo(eventPayload))
          )
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.sendEvent(subscriberUrl, event).unsafeRunSync() shouldBe Delivered
    }

    TooManyRequests +: ServiceUnavailable +: Nil foreach { status =>
      s"return ServiceBusy if remote responds with $status" in new TestCase {

        val (eventJson, eventPayload) = expectEventEncoding(event)

        stubFor {
          post("/")
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(equalToJson(eventJson.spaces2))
            )
            .withMultipartRequestBody(
              aMultipart("payload")
                .withBody(equalTo(eventPayload))
            )
            .willReturn(aResponse().withStatus(TooManyRequests.code))
        }

        sender.sendEvent(subscriberUrl, event).unsafeRunSync() shouldBe ServiceBusy
      }
    }

    NotFound +: BadGateway +: Nil foreach { status =>
      s"return Misdelivered if remote responds with $status" in new TestCase {

        val (eventJson, eventPayload) = expectEventEncoding(event)

        stubFor {
          post("/")
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(equalToJson(eventJson.spaces2))
            )
            .withMultipartRequestBody(
              aMultipart("payload")
                .withBody(equalTo(eventPayload))
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        sender.sendEvent(subscriberUrl, event).unsafeRunSync() shouldBe Misdelivered
      }
    }

    "return Misdelivered if call to the remote fails with Connect Exception" in new TestCase {
      override val sender = new EventsSenderImpl(categoryEventEncoder, TestLogger(), retryInterval = 10 millis)

      expectEventEncoding(event)

      sender
        .sendEvent(SubscriberUrl("http://unexisting"), event)
        .unsafeRunSync() shouldBe Misdelivered
    }

    s"fail if remote responds with $BadRequest" in new TestCase {

      val (eventJson, eventPayload) = expectEventEncoding(event)
      stubFor {
        post("/")
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(eventJson.spaces2))
          )
          .withMultipartRequestBody(
            aMultipart("payload")
              .withBody(equalTo(eventPayload))
          )
          .willReturn(badRequest().withBody("message"))
      }

      intercept[Exception] {
        sender.sendEvent(subscriberUrl, event).unsafeRunSync()
      }.getMessage shouldBe s"POST $subscriberUrl returned $BadRequest; body: message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val event                = testCategoryEvents.generateOne
    val subscriberUrl        = SubscriberUrl(externalServiceBaseUrl)
    val categoryEventEncoder = mock[EventEncoder[TestCategoryEvent]]

    val sender = new EventsSenderImpl[TestCategoryEvent](categoryEventEncoder, TestLogger())

    def expectEventEncoding(event: TestCategoryEvent) = {
      val eventJson    = jsons.generateOne
      val eventPayload = nonEmptyStrings().generateOne
      (categoryEventEncoder.encodeEvent _)
        .expects(event)
        .returning(eventJson)

      (categoryEventEncoder.encodePayload _)
        .expects(event)
        .returning(eventPayload.some)
      (eventJson, eventPayload)
    }
  }

}
