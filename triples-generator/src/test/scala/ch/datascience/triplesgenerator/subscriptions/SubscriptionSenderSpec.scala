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
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class SubscriptionSenderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "send" should {

    s"succeed if posting Subscription URL results with $Accepted" in new TestCase {

      stubFor {
        post("/events/subscriptions?status=READY")
          .withRequestBody(equalToJson(subscriptionUrl.asJson.spaces2))
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      sender.send(subscriptionUrl).unsafeRunSync() shouldBe ((): Unit)
    }

    "fail when posting Subscription URL results in any other status" in new TestCase {

      val message = "message"
      stubFor {
        post("/events/subscriptions?status=READY")
          .withRequestBody(equalToJson(subscriptionUrl.asJson.spaces2))
          .willReturn(badRequest().withBody(message))
      }

      intercept[Exception] {
        sender.send(subscriptionUrl).unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/events/subscriptions?status=READY returned $BadRequest; body: $message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val subscriptionUrl = subscriptionUrls.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val sender      = new IOSubscriptionSender(eventLogUrl, TestLogger())
  }

  private implicit lazy val urlEncoder: Encoder[SubscriptionUrl] = Encoder.instance[SubscriptionUrl] { url =>
    json"""{
      "url": ${url.value}
    }"""
  }
}
