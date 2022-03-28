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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.{SubscriptionMechanism, subscriberUrls}
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.generators.CommonGraphGenerators.{microserviceIdentifiers, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.version.ServiceVersion
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "tryHandling" should {

    "decode an event from the request, " +
      "check if the requested version matches this TG version " +
      s"and return $Accepted" in new TestCase {

        handler.tryHandling(generatePayload(serviceVersion)).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(Info(show"$categoryName: $serviceVersion -> $Accepted"))
      }

    s"fail with $BadRequest for malformed event" in new TestCase {

      handler
        .tryHandling(EventRequestContent.NoPayload(json"""{"categoryName": ${categoryName.value}}"""))
        .unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    "decode an event from the request, " +
      s"and return $BadRequest check if the requested version is different than this TG version" in new TestCase {

        val requestedVersion = serviceVersions.generateOne
        handler.tryHandling(generatePayload(requestedVersion)).unsafeRunSync() shouldBe BadRequest

        logger.loggedOnly(
          Info(show"$categoryName: $requestedVersion -> $BadRequest service in version '$serviceVersion'")
        )
      }
  }

  private trait TestCase {

    val serviceVersion             = serviceVersions.generateOne
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val handler = new EventHandler[IO](serviceVersion, subscriptionMechanism, concurrentProcessesLimiter)

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    (concurrentProcessesLimiter.tryExecuting _).expects(*).onCall { (eventHandlingProcess: EventHandlingProcess[IO]) =>
      eventHandlingProcess.process.merge
    }
  }

  private def generatePayload(version: ServiceVersion) = EventRequestContent.NoPayload(json"""{
    "categoryName": "TS_MIGRATION_REQUEST",
    "subscriber": {
      "url":     ${subscriberUrls.generateOne.value},
      "id":      ${microserviceIdentifiers.generateOne.value},
      "version": ${version.value}
    }
  }
  """)
}
