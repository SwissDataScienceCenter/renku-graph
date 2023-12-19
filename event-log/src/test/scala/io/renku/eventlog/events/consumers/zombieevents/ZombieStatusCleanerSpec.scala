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

package io.renku.eventlog.events.consumers.zombieevents

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting, GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class ZombieStatusCleanerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with TableDrivenPropertyChecks
    with should.Matchers
    with OptionValues {

  private val now           = Instant.now().truncatedTo(MICROS)
  private val executionDate = executionDates.generateOne
  private val zombieMessage = EventMessage("Zombie Event")

  "cleanZombieStatus" should {

    forAll {
      Table(
        "current status"    -> "after update",
        GeneratingTriples   -> New,
        TransformingTriples -> TriplesGenerated,
        Deleting            -> AwaitingDeletion
      )
    } { (currentStatus, afterUpdateStatus) =>
      s"update event status to $afterUpdateStatus " +
        s"if event has status $currentStatus and so the event in the DB" in testDBResource.use { implicit cfg =>
          for {
            event <- addZombieEvent(currentStatus)

            _ <- findEvent(event.eventId).map(_.value).asserting {
                   _.select(Field.Status, Field.ExecutionDate, Field.Message) shouldBe
                     FoundEvent(currentStatus, executionDate, Some(zombieMessage))
                 }

            _ <- updater
                   .cleanZombieStatus(ZombieEvent(event.eventId, event.project.slug, currentStatus))
                   .asserting(_ shouldBe Updated)

            _ <- findEvent(event.eventId).map(_.value).asserting {
                   _.select(Field.Status, Field.ExecutionDate, Field.Message) shouldBe
                     FoundEvent(afterUpdateStatus, ExecutionDate(now), maybeMessage = None)
                 }
          } yield Succeeded
        }

      s"update event status to $afterUpdateStatus and remove the existing event delivery info " +
        s"if event has status $currentStatus and so the event in the DB" in testDBResource.use { implicit cfg =>
          for {
            event <- addZombieEvent(currentStatus)
            _     <- upsertEventDeliveryInfo(event.eventId)

            _ <- findEvent(event.eventId).map(_.value).asserting {
                   _.select(Field.Status, Field.ExecutionDate, Field.Message) shouldBe
                     FoundEvent(currentStatus, executionDate, Some(zombieMessage))
                 }
            _ <- findAllEventDeliveries.asserting(_.map(_.eventId) shouldBe List(event.eventId))

            _ <- updater
                   .cleanZombieStatus(ZombieEvent(event.eventId, event.project.slug, currentStatus))
                   .asserting(_ shouldBe Updated)

            _ <- findEvent(event.eventId).map(_.value).asserting {
                   _.select(Field.Status, Field.ExecutionDate, Field.Message) shouldBe
                     FoundEvent(afterUpdateStatus, ExecutionDate(now), maybeMessage = None)
                 }
            _ <- findAllEventDeliveries.asserting(_ shouldBe Nil)
          } yield Succeeded
        }
    }

    "do nothing if the event does not exists" in testDBResource.use { implicit cfg =>
      for {
        event <- addZombieEvent(GeneratingTriples)
        _ <- findEvent(event.eventId).map(_.value).asserting {
               _.select(Field.Status, Field.ExecutionDate, Field.Message) shouldBe
                 FoundEvent(GeneratingTriples, executionDate, Some(zombieMessage))
             }

        otherEventId = compoundEventIds.generateOne
        _ <- updater
               .cleanZombieStatus(ZombieEvent(otherEventId, event.project.slug, GeneratingTriples))
               .asserting(_ shouldBe NotUpdated)

        _ <- findEvent(event.eventId).map(_.value).asserting {
               _.select(Field.Status, Field.ExecutionDate, Field.Message) shouldBe
                 FoundEvent(GeneratingTriples, executionDate, Some(zombieMessage))
             }
      } yield Succeeded
    }
  }

  private def updater(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new ZombieStatusCleanerImpl[IO](() => now)
  }

  private def addZombieEvent(status: EventStatus)(implicit cfg: DBConfig[EventLogDB]) =
    storeGeneratedEvent(status, executionDate = executionDate, message = Some(zombieMessage))
}
