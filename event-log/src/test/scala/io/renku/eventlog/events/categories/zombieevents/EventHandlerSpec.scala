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
import cats.effect.concurrent.Deferred
import cats.syntax.all._
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, jsons}
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.metrics.LabeledGauge
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "handle" should {

    List(Updated, NotUpdated) foreach { result =>
      s"decode an event with the $GeneratingTriples status from the request, " +
        "schedule event update " +
        s"and return $Accepted if the event status cleaning returned $result" in new TestCase {

          val eventId     = compoundEventIds.generateOne
          val projectPath = projectPaths.generateOne
          val event       = GeneratingTriplesZombieEvent(eventId, projectPath)

          (zombieStatusCleaner.cleanZombieStatus _)
            .expects(event)
            .returning(result.pure[IO])

          if (result == Updated) {
            (awaitingTriplesGenerationGauge.increment _).expects(event.projectPath).returning(IO.unit)
            (underTriplesGenerationGauge.decrement _).expects(event.projectPath).returning(IO.unit)
          }

          handler.handle(requestContent(event.asJson)).getResult shouldBe Accepted

          eventually {
            logger.loggedOnly(
              Info(
                s"${handler.categoryName}: ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status} -> $Accepted"
              ),
              Info(
                s"${handler.categoryName}: ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status} -> $result"
              )
            )
          }
        }

      s"decode an event  with the $TransformingTriples status from the request, " +
        "schedule event update " +
        s"and return $Accepted if the event status cleaning result is $result" in new TestCase {

          val eventId     = compoundEventIds.generateOne
          val projectPath = projectPaths.generateOne
          val event       = TransformingTriplesZombieEvent(eventId, projectPath)

          (zombieStatusCleaner.cleanZombieStatus _)
            .expects(event)
            .returning(result.pure[IO])

          if (result == Updated) {
            (awaitingTriplesTransformationGauge.increment _).expects(event.projectPath).returning(IO.unit)
            (underTriplesTransformationGauge.decrement _).expects(event.projectPath).returning(IO.unit)
          }

          handler.handle(requestContent(event.asJson)).getResult shouldBe Accepted

          eventually {
            logger.loggedOnly(
              Info(
                s"${handler.categoryName}: ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status} -> $Accepted"
              ),
              Info(
                s"${handler.categoryName}: ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status} -> $result"
              )
            )
          }

        }
    }

    "log an error if event status cleaning fails" in new TestCase {

      val event = events.generateOne

      val exception = exceptions.generateOne
      (zombieStatusCleaner.cleanZombieStatus _)
        .expects(event)
        .returning(exception.raiseError[IO, UpdateResult])

      handler.handle(requestContent(event.asJson)).getResult shouldBe Accepted

      eventually {
        logger.loggedOnly(
          Info(
            s"${handler.categoryName}: ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status} -> $Accepted"
          ),
          Error(
            s"${handler.categoryName}: ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status} -> Failure",
            exception
          )
        )
      }
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": "ZOMBIE_CHASING"
        }"""
      }

      handler.handle(request).getResult shouldBe BadRequest

      logger.expectNoLogs()
    }

    s"return $BadRequest if event status is different than $GeneratingTriples or $TransformingTriples" in new TestCase {

      val event = events.generateOne

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

      handler.handle(request).getResult shouldBe BadRequest

      logger.expectNoLogs()
    }
  }

  private trait TestCase {

    val zombieStatusCleaner                = mock[ZombieStatusCleaner[IO]]
    val logger                             = TestLogger[IO]()
    val awaitingTriplesGenerationGauge     = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesGenerationGauge        = mock[LabeledGauge[IO, projects.Path]]
    val awaitingTriplesTransformationGauge = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesTransformationGauge    = mock[LabeledGauge[IO, projects.Path]]
    val handler = new EventHandler[IO](categoryName,
                                       zombieStatusCleaner,
                                       awaitingTriplesGenerationGauge,
                                       underTriplesGenerationGauge,
                                       awaitingTriplesTransformationGauge,
                                       underTriplesTransformationGauge,
                                       logger
    )

    def requestContent(event: Json): EventRequestContent = EventRequestContent(event, None)
  }

  private lazy val events: Gen[ZombieEvent] = for {
    eventId     <- compoundEventIds
    projectPath <- projectPaths
    event <- Gen.oneOf(GeneratingTriplesZombieEvent(eventId, projectPath),
                       TransformingTriplesZombieEvent(eventId, projectPath)
             )
  } yield event

  private implicit def eventEncoder[E <: ZombieEvent]: Encoder[E] = Encoder.instance[E] { event =>
    json"""{
      "categoryName": "ZOMBIE_CHASING",
      "id":           ${event.eventId.id.value},
      "project": {
        "id":         ${event.eventId.projectId.value},
        "path":       ${event.projectPath.value}
      },
      "status":       ${event.status.value}
    }"""
  }

  private implicit class HandlerOps(handlerResult: IO[(Deferred[IO, Unit], IO[EventSchedulingResult])]) {
    lazy val getResult = handlerResult.unsafeRunSync()._2.unsafeRunSync()
  }
}
