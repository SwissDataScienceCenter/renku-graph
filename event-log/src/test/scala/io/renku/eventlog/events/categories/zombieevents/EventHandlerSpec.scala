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

package io.renku.eventlog.events.categories
package zombieevents

import cats.effect.IO
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.EventsGenerators.compoundEventIds
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.metrics.LabeledGauge
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
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

          handler
            .createHandlingProcess(requestContent(event.asJson))
            .unsafeRunSync()
            .process
            .value
            .unsafeRunSync() shouldBe Right(Accepted)

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

          handler
            .createHandlingProcess(requestContent(event.asJson))
            .unsafeRunSync()
            .process
            .value
            .unsafeRunSync() shouldBe Right(
            Accepted
          )

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

      handler
        .createHandlingProcess(requestContent(event.asJson))
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Right(Accepted)

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

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(BadRequest)

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

      handler.createHandlingProcess(request).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(BadRequest)

      logger.expectNoLogs()
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val zombieStatusCleaner                = mock[ZombieStatusCleaner[IO]]
    val awaitingTriplesGenerationGauge     = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesGenerationGauge        = mock[LabeledGauge[IO, projects.Path]]
    val awaitingTriplesTransformationGauge = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesTransformationGauge    = mock[LabeledGauge[IO, projects.Path]]
    val handler = new EventHandler[IO](categoryName,
                                       zombieStatusCleaner,
                                       awaitingTriplesGenerationGauge,
                                       underTriplesGenerationGauge,
                                       awaitingTriplesTransformationGauge,
                                       underTriplesTransformationGauge
    )
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
}
