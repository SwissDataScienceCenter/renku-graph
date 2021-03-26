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

package ch.datascience.webhookservice

import WebhookServiceGenerators._
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.model.CommitSyncRequest
import com.github.tomakehurst.wiremock.client.WireMock.{post, _}
import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.stubbing.Scenario
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class CommitSyncRequestSenderSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

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
          .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
      }

      stubFor {
        postRequest
          .whenScenarioStateIs("Error")
          .willSetStateTo("Successful")
          .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
      }

      stubFor {
        postRequest
          .whenScenarioStateIs("Successful")
          .willReturn(aResponse().withStatus(Accepted.code))
      }

      eventSender.sendCommitSyncRequest(syncRequest).unsafeRunSync() shouldBe ()
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val syncRequest = commitSyncRequests.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val logger      = TestLogger[IO]()
    val eventSender = new CommitSyncRequestSenderImpl(eventLogUrl, logger, 500 millis)
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
