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

package io.renku.webhookservice

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import com.github.tomakehurst.wiremock.stubbing.Scenario
import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.config.EventLogUrl
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.model.CommitSyncRequest
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class CommitSyncRequestSenderSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with IOSpec {

  "send" should {

    s"succeed when delivering the event to the Event Log got $Accepted" in new TestCase {

      stubFor {
        post("/events")
          .withMultipartRequestBody(
            aMultipart("event").withBody(equalToJson(syncRequest.asJson.spaces2))
          )
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      eventSender.sendCommitSyncRequest(syncRequest).unsafeRunSync() shouldBe ()

      logger.loggedOnly(
        Info(
          s"CommitSyncRequest sent for projectId = ${syncRequest.project.id}, projectPath = ${syncRequest.project.path}"
        )
      )
    }

    s"fail when delivering the event to the Event Log got $BadRequest" in new TestCase {

      val status = BadRequest
      stubFor {
        post("/events")
          .withMultipartRequestBody(
            aMultipart("event").withBody(equalToJson(syncRequest.asJson.spaces2))
          )
          .willReturn(aResponse().withStatus(BadRequest.code))
      }

      intercept[Exception] {
        eventSender.sendCommitSyncRequest(syncRequest).unsafeRunSync()
      }.getMessage shouldBe s"POST $eventLogUrl/events returned $status; body: "
    }

    Set(BadGateway, ServiceUnavailable, GatewayTimeout) foreach { errorStatus =>
      s"retry if remote responds with status such as $errorStatus" in new TestCase {

        val postRequest = post("/events").inScenario("Retry")

        stubFor {
          postRequest
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("Error")
            .willReturn(aResponse().withStatus(errorStatus.code))
        }

        stubFor {
          postRequest
            .whenScenarioStateIs("Error")
            .willSetStateTo("Successful")
            .willReturn(aResponse().withStatus(errorStatus.code))
        }

        stubFor {
          postRequest
            .whenScenarioStateIs("Successful")
            .willReturn(aResponse().withStatus(Accepted.code))
        }

        eventSender.sendCommitSyncRequest(syncRequest).unsafeRunSync() shouldBe ()
      }
    }

    "retry if remote is not reachable" in new TestCase {

      val postRequest = post("/events").inScenario("Retry")

      stubFor {
        postRequest
          .whenScenarioStateIs(Scenario.STARTED)
          .willSetStateTo("Error")
          .willReturn(aResponse().withFault(CONNECTION_RESET_BY_PEER))
      }

      stubFor {
        postRequest
          .whenScenarioStateIs("Error")
          .willSetStateTo("Successful")
          .willReturn(aResponse().withFault(CONNECTION_RESET_BY_PEER))
      }

      stubFor {
        postRequest
          .whenScenarioStateIs("Successful")
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      eventSender.sendCommitSyncRequest(syncRequest).unsafeRunSync() shouldBe ()
    }

    "retry in case of client exception" in new TestCase {

      val postRequest = post("/events").inScenario("Retry")

      stubFor {
        postRequest
          .whenScenarioStateIs(Scenario.STARTED)
          .willSetStateTo("Error")
          .willReturn(aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt))
      }

      stubFor {
        postRequest
          .whenScenarioStateIs("Error")
          .willSetStateTo("Successful")
          .willReturn(aResponse().withFault(CONNECTION_RESET_BY_PEER))
      }

      stubFor {
        postRequest
          .whenScenarioStateIs("Successful")
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      eventSender.sendCommitSyncRequest(syncRequest).unsafeRunSync() shouldBe ()
    }
  }

  private trait TestCase {

    val syncRequest = commitSyncRequests.generateOne

    val eventLogUrl     = EventLogUrl(externalServiceBaseUrl)
    implicit val logger = TestLogger[IO]()
    val requestTimeout  = 2 seconds
    val eventSender = new CommitSyncRequestSenderImpl[IO](eventLogUrl,
                                                          500 millis,
                                                          retryInterval = 100 millis,
                                                          maxRetries = 1,
                                                          requestTimeoutOverride = Some(requestTimeout)
    )
  }

  private implicit lazy val eventEncoder: Encoder[CommitSyncRequest] = Encoder.instance[CommitSyncRequest] { event =>
    json"""{
      "categoryName": "COMMIT_SYNC_REQUEST",
      "project": {
        "id":      ${event.project.id.value},
        "path":    ${event.project.path.value}
      }
    }"""
  }
}
