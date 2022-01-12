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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, Project}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, zippedEventPayloads}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{CompoundEventId, ZippedEventPayload}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule triples generation " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        val triplesGeneratedEvent = triplesGeneratedEvents.generateOne
        (eventBodyDeserializer.toEvent _)
          .expects(eventId, project, zippedPayload)
          .returning(triplesGeneratedEvent.pure[IO])

        (eventProcessor.process _)
          .expects(triplesGeneratedEvent)
          .returning(().pure[IO])

        val requestContent: EventRequestContent =
          requestContent((eventId, project).asJson(eventEncoder), zippedPayload)

        handler.createHandlingProcess(requestContent).unsafeRunSyncProcess() shouldBe Right(Accepted)

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $Accepted"
          )
        )
      }

    s"return $BadRequest if event payload is not present" in new TestCase {

      val requestContent = EventRequestContent.NoPayload((eventId, project).asJson(eventEncoder))

      handler.createHandlingProcess(requestContent).unsafeRunSyncProcess() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }

    s"return $BadRequest if event payload is malformed" in new TestCase {

      val requestContent =
        EventRequestContent.WithPayload((eventId, project).asJson(eventEncoder), jsons.generateOne.noSpaces)

      handler.createHandlingProcess(requestContent).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(
        BadRequest
      )

      logger.expectNoLogs()
    }

    s"return $Accepted when event processor fails processing the event" in new TestCase {

      val triplesGeneratedEvent = triplesGeneratedEvents.generateOne
      (eventBodyDeserializer.toEvent _)
        .expects(eventId, project, zippedPayload)
        .returning(triplesGeneratedEvent.pure[IO])

      (eventProcessor.process _)
        .expects(triplesGeneratedEvent)
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      val requestContent: EventRequestContent = requestContent((eventId, project).asJson(eventEncoder), zippedPayload)

      handler.createHandlingProcess(requestContent).unsafeRunSyncProcess() shouldBe Right(Accepted)

      logger.loggedOnly(
        Info(
          s"${handler.categoryName}: $eventId, projectPath = ${triplesGeneratedEvent.project.path} -> $Accepted"
        )
      )
    }
  }

  private trait TestCase {

    val eventId       = compoundEventIds.generateOne
    val zippedPayload = zippedEventPayloads.generateOne
    val projectPath   = projectPaths.generateOne
    val project       = Project(eventId.projectId, projectPath)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventProcessor             = mock[EventProcessor[IO]]
    val eventBodyDeserializer      = mock[EventBodyDeserializer[IO]]
    val concurrentProcessesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val subscriptionMechanism      = mock[SubscriptionMechanism[IO]]
    val handler = new EventHandler[IO](categoryName,
                                       eventBodyDeserializer,
                                       subscriptionMechanism,
                                       concurrentProcessesLimiter,
                                       eventProcessor
    )

    (subscriptionMechanism.renewSubscription _).expects().returns(IO.unit)

    def requestContent(event: Json, payload: ZippedEventPayload): EventRequestContent =
      events.EventRequestContent.WithPayload(event, payload)
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

  private implicit class EventHandlingProcessOps(handlingProcess: IO[EventHandlingProcess[IO]]) {
    def unsafeRunSyncProcess() = handlingProcess.unsafeRunSync().process.value.unsafeRunSync()
  }
}
