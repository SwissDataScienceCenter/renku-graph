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

package io.renku.eventlog.subscriptions

import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.controllers.InfoMessage.InfoMessage
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class SubscriptionsEndpointSpec extends WordSpec with MockFactory {

  "addSubscription" should {

    s"return $Accepted when subscription URL was added to the pool" in new TestCase {

      (subscriptions.add _)
        .expects(subscriptionUrl)
        .returning(IO.unit)

      val request = Request(Method.POST, uri"events" / "subscriptions" withQueryParam ("status", "READY"))
        .withEntity(subscriptionUrl.asJson)

      val response = addSubscription(request).unsafeRunSync()

      response.status                        shouldBe Accepted
      response.contentType                   shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync shouldBe InfoMessage("Subscription added")

      logger.expectNoLogs()
    }

    s"return $BadRequest when subscription URL cannot be decoded from the request" in new TestCase {

      val payload = json"""{}"""
      val request = Request(Method.POST, uri"events" / "subscriptions" withQueryParam ("status", "READY"))
        .withEntity(payload)

      val response = addSubscription(request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync shouldBe ErrorMessage(
        s"Invalid message body: Could not decode JSON: $payload"
      )

      logger.expectNoLogs()
    }

    s"return $InternalServerError when adding subscription URL to the pool fails" in new TestCase {

      val exception = exceptions.generateOne
      (subscriptions.add _)
        .expects(subscriptionUrl)
        .returning(exception.raiseError[IO, Unit])

      val request = Request(Method.POST, uri"events" / "subscriptions" withQueryParam ("status", "READY"))
        .withEntity(subscriptionUrl.asJson)

      val response = addSubscription(request).unsafeRunSync()

      val expectedMessage = "Adding subscription URL failed"
      response.status                         shouldBe InternalServerError
      response.contentType                    shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync shouldBe ErrorMessage(expectedMessage)

      logger.loggedOnly(Error(expectedMessage, exception))
    }
  }

  private trait TestCase {
    val subscriptionUrl = subscriptionUrls.generateOne

    val subscriptions   = mock[TestIOSubscriptions]
    val logger          = TestLogger[IO]()
    val addSubscription = new SubscriptionsEndpoint[IO](subscriptions, logger).addSubscription _
  }

  private implicit lazy val subscriptionUrlEncoder: Encoder[SubscriptionUrl] =
    Encoder.instance[SubscriptionUrl] { url =>
      json"""{
        "url": ${url.value}
      }"""
    }
}
