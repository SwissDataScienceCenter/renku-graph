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
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventStatus}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.{CreatedDate, EventDate, EventMessage, ExecutionDate, InMemoryEventLogDbSpec, TypeSerializers}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import ch.datascience.graph.model.EventsGenerators._
import io.renku.eventlog.EventContentGenerators._
import org.scalatest.wordspec.AnyWordSpec
import doobie.implicits._

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

        val eventId = compoundEventIds.generateOne
        addEvent(eventId, GeneratingTriples)

        findEventStatus(eventId) shouldBe GeneratingTriples

        // verification of the relevant gauges needs to be checked
//        waitingEventsGauge

        updater.changeStatus(ZombieEvent(eventId, GeneratingTriples)).unsafeRunSync() shouldBe ()

        findEventStatus(eventId) shouldBe New
      }
  }

  private trait TestCase {
    val currentTime        = mockFunction[Instant]
    val waitingEventsGauge = mock[LabeledGauge[IO, projects.Path]]
    val queriesExecTimes   = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val updater            = new EventStatusUpdaterImpl(transactor, waitingEventsGauge, queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now)
  }

  private def addEvent(eventId: CompoundEventId, status: EventStatus): Unit =
    storeEvent(eventId, status, executionDates.generateOne, eventDates.generateOne, eventBodies.generateOne)

  def findEventStatus(compoundEventId: CompoundEventId): EventStatus = execute {
    sql"""|SELECT status
          |FROM event  
          |WHERE event_id = ${compoundEventId.id} AND project_id = ${compoundEventId.projectId}
          |""".stripMargin
      .query[EventStatus]
      .unique
  }
}
