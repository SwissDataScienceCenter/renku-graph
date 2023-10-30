/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.events.producers

import EventSender.EventContext
import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import com.github.tomakehurst.wiremock.stubbing.Scenario
import eu.timepit.refined.auto._
import io.renku.events.CategoryName
import io.renku.events.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptyStrings, positiveInts}
import io.renku.graph.config.EventConsumerUrl
import io.renku.graph.metrics.SentEventsGauge
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, NotFound, ServiceUnavailable, TooManyRequests}
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class EventSenderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with MockFactory
    with should.Matchers
    with OptionValues
    with TableDrivenPropertyChecks {

  forAll {
    Table(
      "Request Content Type" -> "SendEvent generator",
      "No Payload" -> { (sender: EventSender[IO], categoryName: CategoryName) =>
        eventRequestContentNoPayloads.map(ev =>
          sender.sendEvent(ev, EventContext(categoryName, nonEmptyStrings().generateOne, maxRetriesNumber = 3))
        )
      },
      "With Payload" -> { (sender: EventSender[IO], categoryName: CategoryName) =>
        eventRequestContentWithZippedPayloads.map(ev =>
          sender.sendEvent(ev, EventContext(categoryName, nonEmptyStrings().generateOne, maxRetriesNumber = 3))
        )
      }
    )
  } { (eventType, callGenerator) =>
    s"sendEvent - $eventType" should {
      Set(Accepted, NotFound) foreach { status =>
        s"succeed if remote responds with status such as $status" in new TestCase {
          val eventRequest = post(urlEqualTo(s"/events"))
          stubFor {
            eventRequest.willReturn(aResponse().withStatus(status.code))
          }

          if (status == Accepted)
            (sentEventsGauge.increment _).expects(categoryName).returning(().pure[IO])

          callGenerator(eventSender, categoryName).generateOne.unsafeRunSync() shouldBe ()

          reset()
        }
      }

      Set(TooManyRequests, BadGateway, ServiceUnavailable, GatewayTimeout) foreach { errorStatus =>
        s"retry if remote responds with status such as $errorStatus" in new TestCase {

          val eventRequest = post(urlEqualTo(s"/events")).inScenario("Retry")

          stubFor {
            eventRequest
              .whenScenarioStateIs(Scenario.STARTED)
              .willSetStateTo("Error")
              .willReturn(aResponse().withStatus(errorStatus.code))
          }

          stubFor {
            eventRequest
              .whenScenarioStateIs("Error")
              .willSetStateTo("Successful")
              .willReturn(aResponse().withStatus(errorStatus.code))
          }

          stubFor {
            eventRequest
              .whenScenarioStateIs("Successful")
              .willReturn(aResponse().withStatus(Accepted.code))
          }

          (sentEventsGauge.increment _).expects(categoryName).returning(().pure[IO])

          callGenerator(eventSender, categoryName).generateOne.unsafeRunSync() shouldBe ()

          reset()
        }
      }

      val failureResponses = List(
        "connection error"   -> aResponse().withFault(CONNECTION_RESET_BY_PEER),
        "other client error" -> aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt)
      )

      failureResponses foreach { case (responseName, failureResponse) =>
        s"retry in case of $responseName" in new TestCase {

          val eventRequest = post(urlEqualTo(s"/events"))
            .inScenario("Retry")

          stubFor {
            eventRequest
              .whenScenarioStateIs(Scenario.STARTED)
              .willSetStateTo("Error")
              .willReturn(failureResponse)
          }

          stubFor {
            eventRequest
              .whenScenarioStateIs("Error")
              .willSetStateTo("Successful")
              .willReturn(failureResponse)
          }

          stubFor {
            eventRequest
              .whenScenarioStateIs("Successful")
              .willReturn(aResponse().withStatus(Accepted.code))
          }

          (sentEventsGauge.increment _).expects(categoryName).returning(().pure[IO])

          callGenerator(eventSender, categoryName).generateOne.unsafeRunSync() shouldBe ()

          reset()
        }
      }

      "fail when the specified number of retries is exceeded" in new TestCase {

        val eventRequest = post(urlEqualTo(s"/events"))
        val response     = aResponse().withFault(CONNECTION_RESET_BY_PEER)

        stubFor {
          eventRequest.willReturn(response)
        }

        (sentEventsGauge.increment _).expects(categoryName).returning(().pure[IO]).anyNumberOfTimes()

        intercept[Exception](
          callGenerator(eventSender, categoryName).generateOne.unsafeRunSync()
        ).getMessage should endWith("no more attempts left")
      }
    }
  }

  "EventContext.nextAttempt" should {

    "return a copy of the context object with decremented number of retries" in {

      val initialRetries = positiveInts().generateOne.value
      val ctx            = generateEventContext(maxRetries = initialRetries.some)

      ctx.retries.value.max  shouldBe initialRetries
      ctx.retries.value.left shouldBe initialRetries

      val nextAttempt = ctx.nextAttempt()
      nextAttempt.retries.value.max  shouldBe initialRetries
      nextAttempt.retries.value.left shouldBe initialRetries - 1
    }

    "return the same instance if no retries specified" in {

      val ctx = generateEventContext(maxRetries = None)

      ctx.retries shouldBe None

      ctx.nextAttempt() shouldBe ctx
    }
  }

  "EventContext.hasAttemptsLeft" should {

    "return true when the number of retries is > 0" in {
      generateEventContext(maxRetries = positiveInts().generateSome.map(_.value)).hasAttemptsLeft shouldBe true
    }

    "return false when the number of retries is <= 0" in {
      generateEventContext(maxRetries = 0.some).hasAttemptsLeft shouldBe false
    }

    "return true when no retries specified" in {
      generateEventContext(maxRetries = None).hasAttemptsLeft shouldBe true
    }
  }

  private lazy val requestTimeout: FiniteDuration = 500 millis

  private trait TestCase {
    val categoryName = categoryNames.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val sentEventsGauge = mock[SentEventsGauge[IO]]
    private val eventConsumerUrl = new EventConsumerUrl {
      override val value: String = externalServiceBaseUrl
    }
    val eventSender = new EventSenderImpl[IO](
      eventConsumerUrl,
      sentEventsGauge,
      onBusySleep = 200 millis,
      onErrorSleep = 500 millis,
      retryInterval = 100 millis,
      maxRetries = 2,
      requestTimeoutOverride = Some(requestTimeout)
    )
  }

  private def generateEventContext(maxRetries: Option[Int]) =
    maxRetries match {
      case None =>
        EventContext(categoryNames.generateOne, nonEmptyStrings().generateOne)
      case Some(retries) =>
        EventContext(categoryNames.generateOne, nonEmptyStrings().generateOne, retries)
    }
}
