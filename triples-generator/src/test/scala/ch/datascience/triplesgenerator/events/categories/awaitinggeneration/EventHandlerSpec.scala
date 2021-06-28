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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.events
import ch.datascience.events.EventRequestContent
import ch.datascience.events.Generators._
import ch.datascience.events.consumers.EventSchedulingResult
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.EventProcessingGenerators._
import ch.datascience.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
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

        val commitEvent = commitEvents.generateOne
        (eventBodyDeserializer.toCommitEvent _)
          .expects(eventBody)
          .returning(commitEvent.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(commitEvent, renkuVersionPair.schemaVersion)
          .returning(EventSchedulingResult.Accepted.pure[IO])

        val request = requestContent(commitEvent.compoundEventId.asJson(eventEncoder), eventBody.value.some)

        handler.handle(request).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: ${commitEvent.compoundEventId}, projectPath = ${commitEvent.project.path} -> $Accepted"
          )
        )
      }

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $Busy if event processor returned $Busy" in new TestCase {

        val commitEvent = commitEvents.generateOne
        (eventBodyDeserializer.toCommitEvent _)
          .expects(eventBody)
          .returning(commitEvent.pure[IO])

        (processingRunner.scheduleForProcessing _)
          .expects(commitEvent, renkuVersionPair.schemaVersion)
          .returning(EventSchedulingResult.Busy.pure[IO])

        val request = requestContent(commitEvent.compoundEventId.asJson(eventEncoder), eventBody.value.some)

        handler.handle(request).unsafeRunSync() shouldBe Busy

        logger.expectNoLogs()
      }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      val payload        = jsons.generateOne.asJson
      val requestContent = eventRequestContents.generateOne.copy(event = payload)

      handler.handle(requestContent).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event body is not correct" in new TestCase {

      val request = requestContent(compoundEventIds.generateOne.asJson(eventEncoder), eventBody.value.some)

      (eventBodyDeserializer.toCommitEvent _)
        .expects(eventBody)
        .returning(exceptions.generateOne.raiseError[IO, CommitEvent])

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $BadRequest if event body is not present" in new TestCase {

      val request = requestContent(compoundEventIds.generateOne.asJson(eventEncoder), None)

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $SchedulingError when event processor fails while accepting the event" in new TestCase {

      val commitEvent = commitEvents.generateOne
      (eventBodyDeserializer.toCommitEvent _)
        .expects(eventBody)
        .returning(commitEvent.pure[IO])

      val exception = exceptions.generateOne
      (processingRunner.scheduleForProcessing _)
        .expects(commitEvent, renkuVersionPair.schemaVersion)
        .returning(exception.raiseError[IO, EventSchedulingResult])

      val request = requestContent(commitEvent.compoundEventId.asJson(eventEncoder), eventBody.value.some)

      handler.handle(request).unsafeRunSync() shouldBe SchedulingError(exception)

      logger.loggedOnly(
        Error(
          s"${handler.categoryName}: ${commitEvent.compoundEventId}, projectPath = ${commitEvent.project.path} -> $SchedulingError",
          exception
        )
      )
    }
  }

  private trait TestCase {

    val eventBody = eventBodies.generateOne

    val processingRunner      = mock[EventsProcessingRunner[IO]]
    val eventBodyDeserializer = mock[EventBodyDeserializer[IO]]
    val renkuVersionPair      = renkuVersionPairs.generateOne
    val logger                = TestLogger[IO]()
    val handler               = new EventHandler[IO](categoryName, processingRunner, eventBodyDeserializer, renkuVersionPair, logger)
    def requestContent(event: Json, maybePayload: Option[String]): EventRequestContent =
      events.EventRequestContent(event, maybePayload)
  }

  private implicit lazy val eventEncoder: Encoder[CompoundEventId] =
    Encoder.instance[CompoundEventId] { eventId =>
      json"""{
        "categoryName": "AWAITING_GENERATION",
        "id":           ${eventId.id.value},
        "project": {
          "id" :        ${eventId.projectId.value}
        }
      }"""
    }
}
