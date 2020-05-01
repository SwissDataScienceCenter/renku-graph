/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.statuschange.commands

import java.time.Instant

import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators.{eventDates, executionDates}
import io.renku.eventlog.EventStatus.{Processing, TriplesStore}
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import io.renku.eventlog.{EventStatus, ExecutionDate, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ToTriplesStoreSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "command" should {

    s"set status $TriplesStore on the event with the given id and $Processing status, " +
      "decrement waiting events and under processing gauges for the project " +
      s"and return ${UpdateResult.Updated}" in new TestCase {

      val projectPath = projectPaths.generateOne
      storeEvent(
        eventId,
        EventStatus.Processing,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne,
        batchDate   = eventBatchDate,
        projectPath = projectPath
      )
      storeEvent(
        compoundEventIds.generateOne.copy(id = eventId.id),
        EventStatus.Processing,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne,
        batchDate = eventBatchDate
      )
      storeEvent(
        compoundEventIds.generateOne,
        EventStatus.Processing,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne,
        batchDate = eventBatchDate
      )

      findEvents(status = TriplesStore) shouldBe List.empty

      (underProcessingGauge.decrement _).expects(projectPath).returning(IO.unit)

      val command = ToTriplesStore[IO](eventId, underProcessingGauge, currentTime)

      (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

      findEvents(status = TriplesStore) shouldBe List((eventId, ExecutionDate(now), eventBatchDate))

      histogram.verifyExecutionTimeMeasured(command.query.name)
    }

    EventStatus.all.filterNot(_ == Processing) foreach { eventStatus =>
      s"do nothing when updating event with $eventStatus status " +
        s"and return ${UpdateResult.Conflict}" in new TestCase {

        val executionDate = executionDates.generateOne
        storeEvent(eventId,
                   eventStatus,
                   executionDate,
                   eventDates.generateOne,
                   eventBodies.generateOne,
                   batchDate = eventBatchDate)

        findEvents(status = eventStatus) shouldBe List((eventId, executionDate, eventBatchDate))

        val command = ToTriplesStore[IO](eventId, underProcessingGauge, currentTime)

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Conflict

        val expectedEvents =
          if (eventStatus != TriplesStore) List.empty
          else List((eventId, executionDate, eventBatchDate))
        findEvents(status = TriplesStore) shouldBe expectedEvents

        histogram.verifyExecutionTimeMeasured(command.query.name)
      }
    }
  }

  private trait TestCase {
    val underProcessingGauge = mock[LabeledGauge[IO, projects.Path]]
    val histogram            = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val currentTime          = mockFunction[Instant]
    val eventId              = compoundEventIds.generateOne
    val eventBatchDate       = batchDates.generateOne

    val commandRunner = new StatusUpdatesRunnerImpl(transactor, histogram, TestLogger[IO]())

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
