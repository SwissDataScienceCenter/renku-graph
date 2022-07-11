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
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.config.EventLogUrl
import io.renku.interpreters.TestLogger
import io.renku.metrics.LabeledGauge
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.{Accepted, BadGateway, GatewayTimeout, NotFound, ServiceUnavailable}
import org.scalamock.scalatest.MockFactory
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
    with TableDrivenPropertyChecks {

  forAll {
    Table(
      "Request Content Type" -> "SendEvent generator",
      "No Payload" -> { (sender: EventSender[IO], categoryName: CategoryName) =>
        eventRequestContentNoPayloads.map(ev =>
          sender.sendEvent(ev, EventContext(categoryName, nonEmptyStrings().generateOne))
        )
      },
      "With Payload" -> { (sender: EventSender[IO], categoryName: CategoryName) =>
        eventRequestContentWithZippedPayloads.map(ev =>
          sender.sendEvent(ev, EventContext(categoryName, nonEmptyStrings().generateOne))
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
    }
  }

  private lazy val requestTimeout: FiniteDuration = 500 millis

  private trait TestCase {
    val categoryName = categoryNames.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val sentEventsGauge = mock[LabeledGauge[IO, CategoryName]]
    val eventLogUrl:  EventLogUrl    = EventLogUrl(externalServiceBaseUrl)
    val onErrorSleep: FiniteDuration = 500 millis
    val eventSender = new EventSenderImpl[IO](eventLogUrl,
                                              sentEventsGauge,
                                              onErrorSleep,
                                              retryInterval = 100 millis,
                                              maxRetries = 2,
                                              requestTimeoutOverride = Some(requestTimeout)
    )
  }
}
