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

import EventStatusUpdater._
import cats.Applicative
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.data.ErrorMessage
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import com.github.tomakehurst.wiremock.stubbing.Scenario
import eu.timepit.refined.auto._
import io.circe.literal._
import org.http4s.Status._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class EventStatusUpdaterSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "toTriplesGenerated" should {

    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with $status" in {
        val processingTime = eventProcessingTimes.generateOne

        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(
                  equalToJson(
                    json"""{"status": "TRIPLES_GENERATED", "processingTime": ${processingTime.value}}""".spaces2
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

        updater
          .toTriplesGenerated(eventId, rawTriples, schemaVersion, processingTime)
          .unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in {
      val processingTime = eventProcessingTimes.generateOne
      val status         = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(
                equalToJson(
                  json"""{"status": "TRIPLES_GENERATED", "processingTime": ${processingTime.value}}""".spaces2
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
        updater.toTriplesGenerated(eventId, rawTriples, schemaVersion, processingTime).unsafeRunSync()
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "toTriplesStore" should {

    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with $status" in {
        val processingTime = eventProcessingTimes.generateOne

        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(
                  equalToJson(json"""{"status": "TRIPLES_STORE", "processingTime": $processingTime}""".spaces2)
                )
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.toTriplesStore(eventId, processingTime).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok" in {
      val processingTime = eventProcessingTimes.generateOne
      val status         = BadRequest

      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(
                equalToJson(json"""{"status": "TRIPLES_STORE", "processingTime": $processingTime}""".spaces2)
              )
          )
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.toTriplesStore(eventId, processingTime).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "rollback" should {

    Set(Ok, NotFound) foreach { status =>
      s"succeed if remote responds with $status - case of $New" in {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(equalToJson(json"""{"status": "NEW"}""".spaces2))
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.rollback[New](eventId).unsafeRunSync() shouldBe ((): Unit)
      }

      s"succeed if remote responds with $status - case of $TriplesGenerated" in {
        stubFor {
          patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .withMultipartRequestBody(
              aMultipart("event")
                .withBody(equalToJson(json"""{"status": "TRIPLES_GENERATED"}""".spaces2))
            )
            .willReturn(aResponse().withStatus(status.code))
        }

        updater.rollback[TriplesGenerated](eventId).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    s"fail if remote responds with status different than $Ok - case of $New" in {
      val status = BadRequest
      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(json"""{"status": "NEW"}""".spaces2))
          )
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.rollback[New](eventId).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }

    s"fail if remote responds with status different than $Ok - case of $TriplesGenerated" in {
      val status = BadRequest
      stubFor {
        patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
          .withMultipartRequestBody(
            aMultipart("event")
              .withBody(equalToJson(json"""{"status": "TRIPLES_GENERATED"}""".spaces2))
          )
          .willReturn(aResponse().withStatus(status.code))
      }

      intercept[Exception] {
        updater.rollback[TriplesGenerated](eventId).unsafeRunSync() shouldBe ((): Unit)
      }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
    }
  }

  "toFailure" should {

    GenerationRecoverableFailure +: GenerationNonRecoverableFailure +: TransformationRecoverableFailure +: TransformationNonRecoverableFailure +: Nil foreach {
      eventStatus =>
        Set(Ok, NotFound) foreach { status =>
          s"succeed if remote responds with $status for $eventStatus" in {
            val exception = exceptions.generateOne
            stubFor {
              patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
                .withMultipartRequestBody(
                  aMultipart("event")
                    .withBody(
                      equalToJson(
                        json"""{
                          "status":  ${eventStatus.value},
                          "message": ${ErrorMessage.withStackTrace(exception).value}
                        }""".spaces2
                      )
                    )
                )
                .willReturn(aResponse().withStatus(status.code))
            }

            updater.toFailure(eventId, eventStatus, exception).unsafeRunSync() shouldBe ((): Unit)
          }
        }

        s"fail if remote responds with status different than $Ok for $eventStatus" in {
          val status = BadRequest

          stubFor {
            patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
              .willReturn(aResponse().withStatus(status.code))
          }

          intercept[Exception] {
            updater.toFailure(eventId, eventStatus, exceptions.generateOne).unsafeRunSync()
          }.getMessage shouldBe s"PATCH $eventLogUrl/events/${eventId.id}/${eventId.projectId} returned $status; body: "
        }
    }
  }

  "eventStatusUpdater" should {

    Set(BadGateway, ServiceUnavailable, GatewayTimeout) foreach { errorStatus =>
      s"retry if remote responds with status such as $errorStatus" in {
        Set(
          updater.toTriplesGenerated(eventId, rawTriples, schemaVersion, eventProcessingTimes.generateOne),
          updater.toTriplesStore(eventId, eventProcessingTimes.generateOne),
          updater.rollback[New](eventId),
          updater.toFailure(eventId, failureEventStatuses.generateOne, exceptions.generateOne)
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

    val failureResponses = List(
      "connection error"   -> aResponse().withFault(CONNECTION_RESET_BY_PEER),
      "other client error" -> aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt)
    )
    val updateFunctions = List(
      "toTriplesGenerated" -> updater.toTriplesGenerated(eventId,
                                                         rawTriples,
                                                         schemaVersion,
                                                         eventProcessingTimes.generateOne
      ),
      "toTriplesStore" -> updater.toTriplesStore(eventId, eventProcessingTimes.generateOne),
      "rollback"       -> updater.rollback[New](eventId),
      "toFailure"      -> updater.toFailure(eventId, failureEventStatuses.generateOne, exceptions.generateOne)
    )
    Applicative[List].product(updateFunctions, failureResponses) foreach {
      case ((functionName, updateFunction), (responseName, response)) =>
        s"retry $functionName command in case of $responseName" in {
          val patchRequest = patch(urlEqualTo(s"/events/${eventId.id}/${eventId.projectId}"))
            .inScenario("Retry")

          stubFor {
            patchRequest
              .whenScenarioStateIs(Scenario.STARTED)
              .willSetStateTo("Error")
              .willReturn(response)
          }

          stubFor {
            patchRequest
              .whenScenarioStateIs("Error")
              .willSetStateTo("Successful")
              .willReturn(response)
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

  private implicit lazy val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit lazy val timer: Timer[IO]        = IO.timer(global)

  private lazy val requestTimeout: Duration    = 1 second
  private lazy val eventLogUrl:    EventLogUrl = EventLogUrl(externalServiceBaseUrl)
  private lazy val updater = new EventStatusUpdaterImpl(eventLogUrl,
                                                        categoryNames.generateOne,
                                                        retryDelay = 100 millis,
                                                        TestLogger(),
                                                        retryInterval = 100 millis,
                                                        maxRetries = 2,
                                                        requestTimeoutOverride = Some(requestTimeout)
  )

  private lazy val eventId       = compoundEventIds.generateOne
  private lazy val rawTriples    = jsonLDTriples.generateOne
  private lazy val schemaVersion = projectSchemaVersions.generateOne
}
