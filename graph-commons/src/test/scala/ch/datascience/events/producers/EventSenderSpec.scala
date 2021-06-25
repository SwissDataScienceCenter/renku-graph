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

package ch.datascience.events.producers

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.events.Generators.eventRequestContents
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonBlankStrings
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import com.github.tomakehurst.wiremock.stubbing.Scenario
import eu.timepit.refined.auto._
import org.http4s.Status.{BadGateway, GatewayTimeout, NotFound, Ok, ServiceUnavailable}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class EventSenderSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "sendEvent" should {
    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with status such as $status" in new TestCase {
        val eventRequest = post(urlEqualTo(s"/events"))
        stubFor {
          eventRequest
            .willReturn(aResponse().withStatus(status.code))
        }

        eventSender
          .sendEvent(eventRequestContents.generateOne, nonBlankStrings().generateOne.value)
          .unsafeRunSync() shouldBe ()

        reset()
      }
    }

    Set(BadGateway, ServiceUnavailable, GatewayTimeout) foreach { errorStatus =>
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
            .willReturn(aResponse().withStatus(Ok.code))
        }
        eventSender
          .sendEvent(eventRequestContents.generateOne, nonBlankStrings().generateOne.value)
          .unsafeRunSync() shouldBe ()

        reset()
      }
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
          .willReturn(aResponse().withStatus(Ok.code))
      }

      eventSender
        .sendEvent(eventRequestContents.generateOne, nonBlankStrings().generateOne.value)
        .unsafeRunSync() shouldBe ()

      reset()
    }
  }

  private implicit lazy val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit lazy val timer: Timer[IO]        = IO.timer(global)
  private lazy val requestTimeout: FiniteDuration   = 1 second

  private trait TestCase {
    val eventLogUrl: EventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val logger = TestLogger[IO]()
    val onErrorSleep: FiniteDuration = 1 second

    val eventSender = new EventSenderImpl[IO](eventLogUrl,
                                              logger,
                                              onErrorSleep,
                                              retryInterval = 100 millis,
                                              maxRetries = 2,
                                              requestTimeoutOverride = Some(requestTimeout)
    )
  }
}
