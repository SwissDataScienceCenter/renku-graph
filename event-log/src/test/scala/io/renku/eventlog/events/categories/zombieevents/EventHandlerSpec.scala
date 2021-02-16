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

package io.renku.eventlog.events.categories.zombieevents

import cats.effect.IO
import ch.datascience.events.consumers.EventRequestContent
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.jsons
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "handle" should {

    "decode an event from the request, " +
      "schedule event update " +
      s"and return $Accepted if event processor accepted the event" in new TestCase {

        (eventStatusUpdater.changeStatus _)
          .expects(event)
          .returning(IO.unit)

        handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: ${event.eventId}, status = ${event.status} -> $Accepted"
          )
        )
      }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      handler.handle(requestContent(jsons.generateOne.asJson)).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": "ZOMBIE_CHASING"
        }"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $BadRequest if event status is different than $GeneratingTriples or $TransformingTriples" in new TestCase {

      val request = requestContent {
        event.asJson deepMerge json"""{
          "status": ${Gen
          .oneOf(
            New,
            TriplesGenerated,
            TriplesStore,
            Skipped,
            GenerationRecoverableFailure,
            GenerationNonRecoverableFailure,
            TransformationRecoverableFailure,
            TransformationNonRecoverableFailure
          )
          .generateOne
          .value}}"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val event = events.generateOne

    val eventStatusUpdater = mock[EventStatusUpdater[IO]]
    val logger             = TestLogger[IO]()
    val handler            = new EventHandler[IO](categoryName, eventStatusUpdater, logger)

    def requestContent(event: Json): EventRequestContent = EventRequestContent(event, None)
  }

  private lazy val events: Gen[ZombieEvent] = for {
    eventId <- compoundEventIds
    status  <- Gen.oneOf(GeneratingTriples, TransformingTriples)
  } yield ZombieEvent(eventId, status)

  private implicit lazy val eventEncoder: Encoder[ZombieEvent] = Encoder.instance[ZombieEvent] { event =>
    json"""{
      "categoryName": "ZOMBIE_CHASING",
      "id":           ${event.eventId.id.value},
      "project": {
        "id":         ${event.eventId.projectId.value}
      },
      "status":       ${event.status.value}
    }"""
  }
}
