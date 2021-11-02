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

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus.{GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ZombieStatusCleanerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with TypeSerializers
    with should.Matchers {

  "cleanZombieStatus" should {

    s"update event status to $New " +
      s"if event has status $GeneratingTriples and so the event in the DB" in new TestCase {

        addZombieEvent(GeneratingTriples)

        findEvent(eventId) shouldBe (executionDate, GeneratingTriples, Some(zombieMessage)).some

        updater.cleanZombieStatus(GeneratingTriplesZombieEvent(eventId, projectPath)).unsafeRunSync() shouldBe Updated

        findEvent(eventId) shouldBe (ExecutionDate(now), New, None).some

        queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")
      }

    s"update event status to $TriplesGenerated " +
      s"if event has status $TransformingTriples and so the event in the DB" in new TestCase {
        addZombieEvent(TransformingTriples)

        findEvent(eventId) shouldBe (executionDate, TransformingTriples, Some(zombieMessage)).some

        updater.cleanZombieStatus(TransformingTriplesZombieEvent(eventId, projectPath)).unsafeRunSync() shouldBe Updated

        findEvent(eventId) shouldBe (ExecutionDate(now), TriplesGenerated, None).some

        queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")
      }

    s"update event status to $New and remove the existing event delivery info " +
      s"if event has status $GeneratingTriples and so the event in the DB" in new TestCase {

        addZombieEvent(GeneratingTriples)
        upsertEventDeliveryInfo(eventId)

        findEvent(eventId)          shouldBe (executionDate, GeneratingTriples, Some(zombieMessage)).some
        findAllDeliveries.map(_._1) shouldBe List(eventId)

        updater.cleanZombieStatus(GeneratingTriplesZombieEvent(eventId, projectPath)).unsafeRunSync() shouldBe Updated

        findEvent(eventId)          shouldBe (ExecutionDate(now), New, None).some
        findAllDeliveries.map(_._1) shouldBe Nil

        queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")
      }

    s"update event status to $TriplesGenerated and remove the existing event delivery info " +
      s"if event has status $TransformingTriples and so the event in the DB" in new TestCase {

        addZombieEvent(TransformingTriples)
        upsertEventDeliveryInfo(eventId)

        findEvent(eventId)          shouldBe (executionDate, TransformingTriples, Some(zombieMessage)).some
        findAllDeliveries.map(_._1) shouldBe List(eventId)

        updater.cleanZombieStatus(TransformingTriplesZombieEvent(eventId, projectPath)).unsafeRunSync() shouldBe Updated

        findEvent(eventId)          shouldBe (ExecutionDate(now), TriplesGenerated, None).some
        findAllDeliveries.map(_._1) shouldBe Nil

        queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")
      }

    "do nothing if the event does not exists" in new TestCase {

      val otherEventId = compoundEventIds.generateOne

      addZombieEvent(GeneratingTriples)

      findEvent(eventId) shouldBe (executionDate, GeneratingTriples, Some(zombieMessage)).some

      updater
        .cleanZombieStatus(GeneratingTriplesZombieEvent(otherEventId, projectPath))
        .unsafeRunSync() shouldBe NotUpdated

      findEvent(eventId) shouldBe (executionDate, GeneratingTriples, Some(zombieMessage)).some

      queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")
    }
  }

  private trait TestCase {
    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val updater          = new ZombieStatusCleanerImpl(sessionResource, queriesExecTimes, currentTime)

    val eventId       = compoundEventIds.generateOne
    val projectPath   = projectPaths.generateOne
    val executionDate = executionDates.generateOne
    val zombieMessage = EventMessage("Zombie Event")

    val now = Instant.now()
    currentTime.expects().returning(now)

    def addZombieEvent(status: EventStatus): Unit = storeEvent(eventId,
                                                               status,
                                                               executionDate,
                                                               eventDates.generateOne,
                                                               eventBodies.generateOne,
                                                               projectPath = projectPath,
                                                               maybeMessage = Some(zombieMessage)
    )
  }
}
