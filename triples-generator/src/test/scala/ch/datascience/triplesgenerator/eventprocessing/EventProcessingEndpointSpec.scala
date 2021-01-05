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

package ch.datascience.triplesgenerator.eventprocessing

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.EventsProcessingRunner.EventSchedulingResult
import ch.datascience.triplesgenerator.eventprocessing.EventsProcessingRunner.EventSchedulingResult.Busy
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningStatus
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.triplesgenerator.generators.VersionGenerators._

class EventProcessingEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "processEvent" should {

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        givenReProvisioningStatusSet(false)

        val commitEvents = eventBody.toCommitEvents
        (eventBodyDeserializer.toCommitEvents _)
          .expects(eventBody)
          .returning(commitEvents.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(eventId, commitEvents, renkuVersionPair.schemaVersion)
          .returning(EventSchedulingResult.Accepted.pure[IO])

        val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

        val response = processEvent(request).unsafeRunSync()

        response.status                          shouldBe Accepted
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event accepted for processing")

        logger.loggedOnly(
          Info(s"Event $eventId, projectPath = ${commitEvents.head.project.path} -> ${EventSchedulingResult.Accepted}")
        )
      }

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $TooManyRequests if event processor returned $Busy" in new TestCase {

        givenReProvisioningStatusSet(false)

        val commitEvents = eventBody.toCommitEvents
        (eventBodyDeserializer.toCommitEvents _)
          .expects(eventBody)
          .returning(commitEvents.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(eventId, commitEvents, renkuVersionPair.schemaVersion)
          .returning(EventSchedulingResult.Busy.pure[IO])

        val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

        val response = processEvent(request).unsafeRunSync()

        response.status                          shouldBe TooManyRequests
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Too many events under processing")

        logger.expectNoLogs()
      }

    s"return $ServiceUnavailable if re-provisioning flag set to true" in new TestCase {

      givenReProvisioningStatusSet(true)

      val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

      val response = processEvent(request).unsafeRunSync()

      response.status      shouldBe ServiceUnavailable
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage(
        "Temporarily unavailable: currently re-provisioning"
      )

      logger.expectNoLogs()
    }

    s"return $BadRequest if decoding an event body from the request fails" in new TestCase {

      givenReProvisioningStatusSet(false)

      val payload = jsons.generateOne.asJson
      val request = Request(Method.POST, uri"events").withEntity(payload)

      val response = processEvent(request).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Event deserialization error")

      logger.expectNoLogs()
    }

    s"return $BadRequest if decoding an event from the request fails" in new TestCase {

      givenReProvisioningStatusSet(false)

      val exception = exceptions.generateOne
      (eventBodyDeserializer.toCommitEvents _)
        .expects(eventBody)
        .returning(exception.raiseError[IO, NonEmptyList[CommitEvent]])

      val payload = (eventId -> eventBody).asJson
      val request = Request(Method.POST, uri"events").withEntity(payload)

      val response = processEvent(request).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Event body deserialization error")

      logger.expectNoLogs()
    }

    s"return $InternalServerError when event processor fails while accepting the event" in new TestCase {

      givenReProvisioningStatusSet(false)

      val commitEvents = eventBody.toCommitEvents
      (eventBodyDeserializer.toCommitEvents _)
        .expects(eventBody)
        .returning(commitEvents.pure[IO])

      val exception = exceptions.generateOne
      (processingRunner.scheduleForProcessing _)
        .expects(eventId, commitEvents, renkuVersionPair.schemaVersion)
        .returning(exception.raiseError[IO, EventSchedulingResult])

      val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

      val response = processEvent(request).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Scheduling Event for processing failed").asJson

      logger.loggedOnly(Error("Scheduling Event for processing failed", exception))
    }
  }

  private trait TestCase {
    val eventId          = compoundEventIds.generateOne
    val eventBody        = eventBodies.generateOne
    val renkuVersionPair = renkuVersionPairs.generateOne

    val eventBodyDeserializer = mock[IOEventBodyDeserialiser]
    val processingRunner      = mock[EventsProcessingRunner[IO]]
    val reProvisioningStatus  = mock[ReProvisioningStatus[IO]]
    val logger                = TestLogger[IO]()
    val processEvent = new EventProcessingEndpoint[IO](eventBodyDeserializer,
                                                       processingRunner,
                                                       reProvisioningStatus,
                                                       renkuVersionPair,
                                                       logger
    ).processEvent _

    def givenReProvisioningStatusSet(flag: Boolean) =
      (reProvisioningStatus.isReProvisioning _)
        .expects()
        .returning(flag.pure[IO])
  }

  private implicit lazy val eventEncoder: Encoder[(CompoundEventId, EventBody)] =
    Encoder.instance[(CompoundEventId, EventBody)] { case (eventId, body) =>
      json"""{
        "id":      ${eventId.id.value},
        "project": {
          "id" :   ${eventId.projectId.value}
        },
        "body":    ${body.value}
      }"""
    }

  private implicit class EventBodyOps(eventBody: EventBody) {
    lazy val toCommitEvents: NonEmptyList[CommitEvent] = commitEvents.generateNonEmptyList()
  }
}
