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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import TriplesGeneratedGenerators._
import ch.datascience.triplesgenerator.events.EventSchedulingResult._
import ch.datascience.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.syntax.all._
import org.http4s.{Method, Request}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        val triplesGeneratedEvent = eventBody.toTriplesGeneratedEvent
        (eventBodyDeserializer.toTriplesGeneratedEvent _)
          .expects(eventId, eventBody)
          .returning(triplesGeneratedEvent.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(triplesGeneratedEvent, renkuVersionPair.schemaVersion)
          .returning(EventSchedulingResult.Accepted.pure[IO])

        val request = Request[IO](Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

        handler.handle(request).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $Accepted"
          )
        )
      }

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $Busy if event processor returned $Busy" in new TestCase {

        val triplesGeneratedEvent = eventBody.toTriplesGeneratedEvent
        (eventBodyDeserializer.toTriplesGeneratedEvent _)
          .expects(eventId, eventBody)
          .returning(triplesGeneratedEvent.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(triplesGeneratedEvent, renkuVersionPair.schemaVersion)
          .returning(EventSchedulingResult.Busy.pure[IO])

        val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

        handler.handle(request).unsafeRunSync() shouldBe Busy

        logger.expectNoLogs()
      }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      val payload = jsons.generateOne.asJson
      val request = Request(Method.POST, uri"events").withEntity(payload)

      handler.handle(request).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is of wrong category" in new TestCase {

      val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

      (eventBodyDeserializer.toTriplesGeneratedEvent _)
        .expects(eventId, eventBody)
        .returning(exceptions.generateOne.raiseError[IO, TriplesGeneratedEvent])

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $SchedulingError when event processor fails while accepting the event" in new TestCase {

      val triplesGeneratedEvent = eventBody.toTriplesGeneratedEvent
      (eventBodyDeserializer.toTriplesGeneratedEvent _)
        .expects(eventId, eventBody)
        .returning(triplesGeneratedEvent.pure[IO])

      val exception = exceptions.generateOne
      (processingRunner.scheduleForProcessing _)
        .expects(triplesGeneratedEvent, renkuVersionPair.schemaVersion)
        .returning(exception.raiseError[IO, EventSchedulingResult])

      val request = Request(Method.POST, uri"events").withEntity((eventId -> eventBody).asJson)

      handler.handle(request).unsafeRunSync() shouldBe SchedulingError(exception)

      logger.loggedOnly(
        Error(
          s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $SchedulingError",
          exception
        )
      )
    }
  }

  private trait TestCase {

    val eventId   = compoundEventIds.generateOne
    val eventBody = eventBodies.generateOne

    val processingRunner      = mock[EventsProcessingRunner[IO]]
    val eventBodyDeserializer = mock[EventBodyDeserializer[IO]]
    val renkuVersionPair      = renkuVersionPairs.generateOne
    val logger                = TestLogger[IO]()
    val handler               = new EventHandler[IO](processingRunner, eventBodyDeserializer, renkuVersionPair, logger)
  }

  private implicit lazy val eventEncoder: Encoder[(CompoundEventId, EventBody)] =
    Encoder.instance[(CompoundEventId, EventBody)] { case (eventId, body) =>
      json"""{
        "categoryName": "TRIPLES_GENERATED",
        "id":           ${eventId.id.value},
        "project": {
          "id" :        ${eventId.projectId.value}
        },
        "body":         ${body.value}
      }"""
    }

  private implicit class EventBodyOps(eventBody: EventBody) {
    lazy val toTriplesGeneratedEvent: TriplesGeneratedEvent =
      triplesGeneratedEvents.generateOne
  }
}
