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

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, _}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.metrics.TestLabeledHistogram
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{ExecutionDate, InMemoryEventLogDbSpec, TypeSerializers}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class EventStatusUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with TypeSerializers
    with should.Matchers {

  "changeStatus" should {

    s"update event status to $New " +
      s"if event has status $GeneratingTriples and so the event in the DB" in new TestCase {

        addEvent(GeneratingTriples)

        findEventStatus(eventId) shouldBe (GeneratingTriples, executionDate)

        updater.changeStatus(GeneratingTriplesZombieEvent(eventId, projectPath)).unsafeRunSync() shouldBe Updated

        queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")

        findEventStatus(eventId) shouldBe (New, ExecutionDate(now))

      }

    s"update event status to $TriplesGenerated " +
      s"if event has status $TransformingTriples and so the event in the DB" in new TestCase {
        addEvent(TransformingTriples)

        findEventStatus(eventId) shouldBe (TransformingTriples, executionDate)

        updater.changeStatus(TransformingTriplesZombieEvent(eventId, projectPath)).unsafeRunSync() shouldBe Updated

        queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")

        findEventStatus(eventId) shouldBe (TriplesGenerated, ExecutionDate(now))
      }

    "do nothing if the event does not exists" in new TestCase {

      val otherEventId = compoundEventIds.generateOne

      addEvent(GeneratingTriples)

      findEventStatus(eventId) shouldBe (GeneratingTriples, executionDate)

      updater
        .changeStatus(GeneratingTriplesZombieEvent(otherEventId, projectPath))
        .unsafeRunSync() shouldBe NotUpdated

      queriesExecTimes.verifyExecutionTimeMeasured("zombie_chasing - update status")

      findEventStatus(eventId) shouldBe (GeneratingTriples, executionDate)
    }

  }

  private trait TestCase {
    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val updater          = new EventStatusUpdaterImpl(transactor, queriesExecTimes, currentTime)

    val eventId       = compoundEventIds.generateOne
    val projectPath   = projectPaths.generateOne
    val executionDate = executionDates.generateOne

    val now = Instant.now()
    currentTime.expects().returning(now)

    def addEvent(status: EventStatus): Unit =
      storeEvent(eventId,
                 status,
                 executionDate,
                 eventDates.generateOne,
                 eventBodies.generateOne,
                 projectPath = projectPath
      )

  }

  def findEventStatus(compoundEventId: CompoundEventId): (EventStatus, ExecutionDate) = execute {
    sql"""|SELECT status, execution_date
          |FROM event  
          |WHERE event_id = ${compoundEventId.id} AND project_id = ${compoundEventId.projectId}
          |""".stripMargin
      .query[(EventStatus, ExecutionDate)]
      .unique
  }

}
