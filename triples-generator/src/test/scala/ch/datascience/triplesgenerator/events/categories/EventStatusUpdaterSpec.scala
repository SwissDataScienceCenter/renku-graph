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

package ch.datascience.triplesgenerator.events.categories

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.EventsGenerators.{categoryNames, compoundEventIds, eventProcessingTimes, failureEventStatuses}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.http.ErrorMessage
import ch.datascience.interpreters.TestLogger
import ch.datascience.json.JsonOps._
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.stubbing.Scenario
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class EventStatusUpdaterSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "markNew" should {

    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(equalToJson(json"""{"status": "NEW"}""".spaces2))
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markEventNew(eventId).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val status = BadRequest
      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withRequestBody(equalToJson(json"""{"status": "NEW"}""".spaces2))
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markEventNew(eventId).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markTriplesStore" should {

    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        val processingTime = eventProcessingTimes.generateOne

        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withRequestBody(
              equalToJson(
                json"""{"status": "TRIPLES_STORE", "processingTime": $processingTime}""".spaces2
              )
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.markTriplesStore(eventId, processingTime).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne
      val status         = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withRequestBody(
            equalToJson(
              json"""{"status": "TRIPLES_STORE", "processingTime": $processingTime}""".spaces2
            )
          )
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markTriplesStore(eventId, processingTime).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markTriplesGenerated" should {

    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with $status" in new TestCase {
        val maybeProcessingTime = eventProcessingTimes.generateOption

        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(
                  equalToJson(
                    json"""{"status": "TRIPLES_GENERATED"}"""
                      .addIfDefined("processingTime" -> maybeProcessingTime)
                      .spaces2
                  )
                )
            )
            .withMultipartRequestBody(
              aMultipart("payload")
                .withBody(
                  equalToJson(
                    json"""{"schemaVersion": ${schemaVersion.value} , "payload": ${rawTriples.value.noSpaces} }""".noSpaces
                  )
                )
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater
          .markTriplesGenerated(eventId, rawTriples, schemaVersion, maybeProcessingTime)
          .unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in new TestCase {
      val maybeProcessingTime = eventProcessingTimes.generateOption
      val status              = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(
                equalToJson(
                  json"""{"status": "TRIPLES_GENERATED"}"""
                    .addIfDefined("processingTime" -> maybeProcessingTime)
                    .spaces2
                )
              )
          )
          .withMultipartRequestBody(
            aMultipart("payload")
              .withBody(
                equalToJson(
                  json"""{"schemaVersion": ${schemaVersion.value} , "payload": ${rawTriples.value.noSpaces}}""".noSpaces
                )
              )
          )
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.markTriplesGenerated(eventId, rawTriples, schemaVersion, maybeProcessingTime).unsafeRunSync()
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "markEventFailed" should {

    GenerationRecoverableFailure +: GenerationNonRecoverableFailure +: TransformationRecoverableFailure +: TransformationNonRecoverableFailure +: Nil foreach {
      eventStatus =>
        Set(Ok, NotFound) foreach { status =>
          s"succeed if remote responds with $status for $eventStatus" in new TestCase {
            val exception = exceptions.generateOne
            stubFor {
              patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
                .withRequestBody(
                  equalToJson(
                    json"""{
                      "status":  ${eventStatus.value},
                      "message": ${ErrorMessage(exception).value}
                    }""".spaces2
                  )
                )
                .willReturn(aResponse().withStatus(status.code))
            }

            updater.markEventFailed(eventId, eventStatus, exception).unsafeRunSync() shouldBe ((): Unit)
          }
        }

        s"fail if remote responds with status different than $Ok for $eventStatus" in new TestCase {
          val status = BadRequest

          stubFor {
            patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
              .willReturn(aResponse().withStatus(status.code))
          }

          intercept[Exception] {
            updater.markEventFailed(eventId, eventStatus, exceptions.generateOne).unsafeRunSync()
          }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
        }
    }
  }

  "eventStatusUpdater" should {
    Set(BadGateway, ServiceUnavailable, GatewayTimeout) foreach { errorStatus =>
      s"retry if remote responds with status such as $errorStatus" in new TestCase {
        Set(
          updater.markTriplesGenerated(eventId, rawTriples, schemaVersion, eventProcessingTimes.generateOption),
          updater.markEventNew(eventId),
          updater.markTriplesStore(eventId, eventProcessingTimes.generateOne),
          updater.markEventFailed(eventId, failureEventStatuses.generateOne, exceptions.generateOne)
        ) foreach { updateFunction =>
          val patchRequest = patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .inScenario("Retry")

          stubFor {
            patchRequest
              .whenScenarioStateIs(Scenario.STARTED)
              .willSetStateTo("Error")
              .willReturn(aResponse().withStatus(errorStatus.code))
          }

          stubFor {
            patchRequest
              .whenScenarioStateIs("Error")
              .willSetStateTo("Successful")
              .willReturn(aResponse().withStatus(errorStatus.code))
          }

          stubFor {
            patchRequest
              .whenScenarioStateIs("Successful")
              .willReturn(aResponse().withStatus(Ok.code))
          }

          updateFunction.unsafeRunSync() shouldBe ()

          reset()
        }
      }
    }
    s"retry if remote is not reachable" in new TestCase {
      Set(
        updater.markTriplesGenerated(eventId, rawTriples, schemaVersion, eventProcessingTimes.generateOption),
        updater.markEventNew(eventId),
        updater.markTriplesStore(eventId, eventProcessingTimes.generateOne),
        updater.markEventFailed(eventId, failureEventStatuses.generateOne, exceptions.generateOne)
      ) foreach { updateFunction =>
        val patchRequest = patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .inScenario("Retry")

        stubFor {
          patchRequest
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("Error")
            .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
        }

        stubFor {
          patchRequest
            .whenScenarioStateIs("Error")
            .willSetStateTo("Successful")
            .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
        }

        stubFor {
          patchRequest
            .whenScenarioStateIs("Successful")
            .willReturn(aResponse().withStatus(Ok.code))
        }

        updateFunction.unsafeRunSync() shouldBe ()

        reset()
      }
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val eventId       = compoundEventIds.generateOne
    val rawTriples    = jsonLDTriples.generateOne
    val schemaVersion = projectSchemaVersions.generateOne

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val updater =
      new EventStatusUpdaterImpl(eventLogUrl, categoryNames.generateOne, retryDelay = 500 millis, TestLogger())
  }
}
