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

package ch.datascience.triplesgenerator.subscriptions

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class SubscriptionSenderSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "postToEventLog" should {

    s"succeed if posting Subscriber URL and statuses NEW and GENERATION_RECOVERABLE_FAILURE results with $Accepted" in new TestCase {

      stubFor {
        post("/subscriptions")
          .withRequestBody(equalToJson((subscriberUrl -> Set("NEW", "GENERATION_RECOVERABLE_FAILURE")).asJson.spaces2))
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.postToEventLog(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
    }

    "fail when posting the payload results in any other status" in new TestCase {

      val message = "message"
      stubFor {
        post("/subscriptions")
          .withRequestBody(equalToJson((subscriberUrl -> Set("NEW", "GENERATION_RECOVERABLE_FAILURE")).asJson.spaces2))
          .willReturn(badRequest().withBody(message))
      }

      intercept[Exception] {
        sender.postToEventLog(subscriberUrl).unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/subscriptions returned $BadRequest; body: $message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val subscriberUrl = subscriberUrls.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val sender      = new IOSubscriptionSender(eventLogUrl, TestLogger())
  }

  private implicit lazy val payloadEncoder: Encoder[(SubscriberUrl, Set[String])] =
    Encoder.instance[(SubscriberUrl, Set[String])] { case (url, statuses) =>
      json"""{
        "subscriberUrl": ${url.value},
        "statuses": ${statuses.toList}
      }"""
    }
}
