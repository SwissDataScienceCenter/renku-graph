/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers
package zombieevents

import cats.effect.IO
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.eventlog.metrics.TestEventStatusGauges._
import io.renku.eventlog.metrics.{EventStatusGauges, TestEventStatusGauges}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ProcessExecutor
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, processingStatuses}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus
import io.renku.interpreters.TestLogger
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

  "createHandlingDefinition.decode" should {
    s"decode a valid event successfully" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = events.generateOne
      definition.decode(EventRequestContent(eventData.asJson)) shouldBe Right(eventData)
    }

    "fail on invalid event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Json.obj("invalid" -> true.asJson)
      definition.decode(EventRequestContent(eventData)).isLeft shouldBe true
    }
  }

  "createHandlingDefinition.process" should {
    "call to zombieStatusCleaner and update gauges on Updated result" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (zombieStatusCleaner.cleanZombieStatus _).expects(*).returning(IO.pure(Updated))
      val event = events.generateOne
      definition.process(event).unsafeRunSync() shouldBe ()

      val (incGauge, decGauge) = event.status match {
        case EventStatus.GeneratingTriples   => gauges.awaitingGeneration     -> gauges.underGeneration
        case EventStatus.TransformingTriples => gauges.awaitingTransformation -> gauges.underTransformation
        case EventStatus.Deleting            => gauges.awaitingDeletion       -> gauges.underDeletion
      }

      incGauge.getValue(event.projectSlug).unsafeRunSync() shouldBe 1d
      decGauge.getValue(event.projectSlug).unsafeRunSync() shouldBe -1d
    }

    "call to zombieStatusCleaner and don't update gauges on NotUpdated result" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (zombieStatusCleaner.cleanZombieStatus _).expects(*).returning(IO.pure(NotUpdated))
      val event = events.generateOne
      definition.process(event).unsafeRunSync() shouldBe ()

      val (incGauge, decGauge) = event.status match {
        case EventStatus.GeneratingTriples   => gauges.awaitingGeneration     -> gauges.underGeneration
        case EventStatus.TransformingTriples => gauges.awaitingTransformation -> gauges.underTransformation
        case EventStatus.Deleting            => gauges.awaitingDeletion       -> gauges.underDeletion
      }

      incGauge.getValue(event.projectSlug).unsafeRunSync() shouldBe 0d
      decGauge.getValue(event.projectSlug).unsafeRunSync() shouldBe 0d
    }
  }

  "createHandlingDefinition" should {
    "not define  precondition and onRelease" in new TestCase {
      val definition = handler.createHandlingDefinition()
      definition.precondition.unsafeRunSync() shouldBe None
      definition.onRelease                    shouldBe None
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO]        = TestLogger[IO]()
    implicit val gauges: EventStatusGauges[IO] = TestEventStatusGauges[IO]
    val zombieStatusCleaner = mock[ZombieStatusCleaner[IO]]
    val handler             = new EventHandler[IO](zombieStatusCleaner, ProcessExecutor.sequential[IO])

    val events: Gen[ZombieEvent] = for {
      eventId     <- compoundEventIds
      projectPath <- projectSlugs
      status      <- processingStatuses
    } yield ZombieEvent(eventId, projectPath, status)

    implicit val eventEncoder: Encoder[ZombieEvent] = Encoder.instance { event =>
      json"""{
        "categoryName": "ZOMBIE_CHASING",
        "id": ${event.eventId.id.value},
        "project": {
          "id":   ${event.eventId.projectId.value},
          "path": ${event.projectSlug.value}
       },
        "status": ${event.status.value}
      }"""
    }
  }
}
