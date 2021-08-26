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
import cats.effect.concurrent.Deferred
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventRequestContent, EventSchedulingResult, Project}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
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

        (eventProcessor.process _)
          .expects(triplesGeneratedEvent)
          .returning(().pure[IO])

        val requestContent: EventRequestContent =
          requestContent((eventId, project).asJson(eventEncoder), bodyContent.some)

        handler.handle(requestContent).getResult shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $Accepted"
          )
        )
      }

    s"return $BadRequest if event body is not present" in new TestCase {

      val requestContent: EventRequestContent =
        requestContent((eventId, project).asJson(eventEncoder), None)

      handler.handle(requestContent).getResult shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $BadRequest if event body is malformed" in new TestCase {

      val requestContent: EventRequestContent =
        requestContent((eventId, project).asJson(eventEncoder), jsons.generateOne.noSpaces.some)

      handler.handle(requestContent).getResult shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $Accepted when event processor fails processing the event" in new TestCase {

      val triplesGeneratedEvent = eventBody.toTriplesGeneratedEvent
      (eventBodyDeserializer.toTriplesGeneratedEvent _)
        .expects(eventId, project, schemaVersion, eventBody)
        .returning(triplesGeneratedEvent.pure[IO])

      (eventProcessor.process _)
        .expects(triplesGeneratedEvent)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      val requestContent: EventRequestContent =
        requestContent((eventId, project).asJson(eventEncoder), bodyContent.some)

      handler.handle(requestContent).getResult shouldBe Accepted

      logger.loggedOnly(
        Info(
          s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $Accepted"
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

    val bodyContent = json"""{ "schemaVersion": ${schemaVersion.value}, "payload": ${eventBody.value} }""".noSpaces

    val eventProcessor             = mock[EventProcessor[IO]]
    val eventBodyDeserializer      = mock[EventBodyDeserializer[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val logger                     = TestLogger[IO]()
    val handler =
      new EventHandler[IO](categoryName,
                           eventBodyDeserializer,
                           subscriptionMechanism,
                           concurrentProcessesLimiter,
                           eventProcessor,
                           logger
      )

    def requestContent(event: Json, maybePayload: Option[String]): EventRequestContent =
      EventRequestContent(event, maybePayload)
  }

  private implicit lazy val eventEncoder: Encoder[(CompoundEventId, Project)] =
    Encoder.instance[(CompoundEventId, Project)] { case (eventId, project) =>
      json"""{
        "categoryName": "TRIPLES_GENERATED",
        "id":           ${eventId.id.value},
        "project": {
          "id" :        ${eventId.projectId.value},
          "path": ${project.path.value}
        }
      }"""
    }

  private implicit class EventBodyOps(eventBody: EventBody) {
    lazy val toTriplesGeneratedEvent: TriplesGeneratedEvent =
      triplesGeneratedEvents.generateOne
  }

  private implicit class HandlerOps(handlerResult: IO[(Deferred[IO, Unit], IO[EventSchedulingResult])]) {
    lazy val getResult = handlerResult.unsafeRunSync()._2.unsafeRunSync()
  }
}
