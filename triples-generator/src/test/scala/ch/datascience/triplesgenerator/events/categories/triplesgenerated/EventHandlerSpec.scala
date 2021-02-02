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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult._
import ch.datascience.triplesgenerator.events.IOEventEndpoint.EventRequestContent
import ch.datascience.triplesgenerator.events.categories.models.Project
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
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
          .expects(eventId, project, schemaVersion, eventBody)
          .returning(triplesGeneratedEvent.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(triplesGeneratedEvent)
          .returning(EventSchedulingResult.Accepted.pure[IO])

        val requestContent: EventRequestContent =
          requestContent((eventId, project, schemaVersion).asJson(eventEncoder), eventBody.value.some)

        handler.handle(requestContent).unsafeRunSync() shouldBe Accepted

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
          .expects(eventId, project, schemaVersion, eventBody)
          .returning(triplesGeneratedEvent.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(triplesGeneratedEvent)
          .returning(EventSchedulingResult.Busy.pure[IO])

        val requestContent: EventRequestContent =
          requestContent((eventId, project, schemaVersion).asJson(eventEncoder), eventBody.value.some)

        handler.handle(requestContent).unsafeRunSync() shouldBe Busy

        logger.expectNoLogs()
      }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      val event   = jsons.generateOne.asJson
      val payload = nonEmptyStrings().generateSome
      val request = requestContent(event, payload)

      handler.handle(request).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event body is not present" in new TestCase {

      val requestContent: EventRequestContent =
        requestContent((eventId, project, schemaVersion).asJson(eventEncoder), eventBody.value.some)

      (eventBodyDeserializer.toTriplesGeneratedEvent _)
        .expects(eventId, project, schemaVersion, eventBody)
        .returning(exceptions.generateOne.raiseError[IO, TriplesGeneratedEvent])

      handler.handle(requestContent).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $SchedulingError when event processor fails while accepting the event" in new TestCase {

      val triplesGeneratedEvent = eventBody.toTriplesGeneratedEvent
      (eventBodyDeserializer.toTriplesGeneratedEvent _)
        .expects(eventId, project, schemaVersion, eventBody)
        .returning(triplesGeneratedEvent.pure[IO])

      val exception = exceptions.generateOne
      (processingRunner.scheduleForProcessing _)
        .expects(triplesGeneratedEvent)
        .returning(exception.raiseError[IO, EventSchedulingResult])

      val requestContent: EventRequestContent =
        requestContent((eventId, project, schemaVersion).asJson(eventEncoder), eventBody.value.some)

      handler.handle(requestContent).unsafeRunSync() shouldBe SchedulingError(exception)

      logger.loggedOnly(
        Error(
          s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $SchedulingError",
          exception
        )
      )
    }
  }

  private trait TestCase {

    val eventId       = compoundEventIds.generateOne
    val eventBody     = eventBodies.generateOne
    val projectPath   = projectPaths.generateOne
    val project       = Project(eventId.projectId, projectPath)
    val schemaVersion = projectSchemaVersions.generateOne

    val processingRunner      = mock[EventsProcessingRunner[IO]]
    val eventBodyDeserializer = mock[EventBodyDeserializer[IO]]
    val logger                = TestLogger[IO]()
    val handler               = new EventHandler[IO](processingRunner, eventBodyDeserializer, logger)

    def requestContent(event: Json, maybePayload: Option[String]): EventRequestContent =
      EventRequestContent(event, maybePayload)
  }

  private implicit lazy val eventEncoder: Encoder[(CompoundEventId, Project, SchemaVersion)] =
    Encoder.instance[(CompoundEventId, Project, SchemaVersion)] { case (eventId, project, schemaVersion) =>
      json"""{
        "categoryName": "TRIPLES_GENERATED",
        "id":           ${eventId.id.value},
        "project": {
          "id" :        ${eventId.projectId.value},
          "path": ${project.path.value}
        },
        "schemaVersion": ${schemaVersion.value}
      }"""
    }

  private implicit class EventBodyOps(eventBody: EventBody) {
    lazy val toTriplesGeneratedEvent: TriplesGeneratedEvent =
      triplesGeneratedEvents.generateOne
  }
}
