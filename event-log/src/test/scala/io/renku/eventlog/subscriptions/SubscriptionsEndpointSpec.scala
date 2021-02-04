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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.ErrorMessage.ErrorMessage
import ch.datascience.http.InfoMessage.InfoMessage
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.renku.eventlog.subscriptions.Generators._
import io.renku.eventlog.subscriptions.SubscriptionCategoryRegistry.{SubscriptionResult, SuccessfulSubscription, UnsupportedPayload}
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionsEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "addSubscription" should {

    s"return $Accepted when the payload is acceptable" +
      "and subscriber URL was added to the pool" in new TestCase {
        val payload = jsons.generateOne
        val request = Request(Method.POST, uri"subscriptions")
          .withEntity(payload)

        (subscriptionCategoryRegistry.register _)
          .expects(payload)
          .returning(SuccessfulSubscription.pure[IO])

        val response = addSubscription(request).unsafeRunSync()

        response.status                          shouldBe Accepted
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Subscription added")

        logger.expectNoLogs()
      }

    s"return $BadRequest when the body of the request is malformed" in new TestCase {
      val request = Request[IO](Method.POST, uri"subscriptions").withEntity("malformedJson")

      val response = addSubscription(request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
        s"Malformed message body: Invalid JSON"
      )

      logger.expectNoLogs()
    }

    s"return $BadRequest when no category accept the subscription" in new TestCase {
      val payload = jsons.generateOne
      val request = Request(Method.POST, uri"subscriptions")
        .withEntity(payload)
      val errorMessage = nonEmptyStrings().generateOne

      (subscriptionCategoryRegistry.register _)
        .expects(payload)
        .returning(UnsupportedPayload(errorMessage).pure[IO])

      val response = addSubscription(request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage)

      logger.expectNoLogs()
    }

    s"return $InternalServerError when registration fails" in new TestCase {

      val exception = exceptions.generateOne

      val payload = jsons.generateOne
      val request = Request(Method.POST, uri"subscriptions")
        .withEntity(payload)

      (subscriptionCategoryRegistry.register _)
        .expects(payload)
        .returning(exception.raiseError[IO, SubscriptionResult])

      val response = addSubscription(request).unsafeRunSync()

      val expectedMessage = "Registering subscriber failed"
      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(expectedMessage)

      logger.loggedOnly(Error(expectedMessage, exception))
    }
  }

  private trait TestCase {
    val subscriptionCategoryRegistry = mock[SubscriptionCategoryRegistry[IO]]
    val logger                       = TestLogger[IO]()
    val addSubscription =
      new SubscriptionsEndpoint[IO](subscriptionCategoryRegistry, logger).addSubscription _
  }
}
