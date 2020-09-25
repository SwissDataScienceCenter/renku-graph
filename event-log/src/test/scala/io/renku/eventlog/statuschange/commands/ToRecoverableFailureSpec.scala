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
import java.time.temporal.ChronoUnit.MINUTES

import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators.{eventDates, eventMessages, executionDates}
import io.renku.eventlog.EventStatus.{Processing, RecoverableFailure}
import io.renku.eventlog._
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ToRecoverableFailureSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "command" should {

    s"set status $RecoverableFailure on the event with the given id and $Processing status, " +
      "increment waiting events gauge and decrement under processing gauge for the project " +
      s"and return ${UpdateResult.Updated}" in new TestCase {

        storeEvent(
          compoundEventIds.generateOne.copy(id = eventId.id),
          EventStatus.Processing,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate
        )
        val executionDate = executionDates.generateOne
        val projectPath   = projectPaths.generateOne
        storeEvent(
          eventId,
          EventStatus.Processing,
          executionDate,
          eventDates.generateOne,
          eventBodies.generateOne,
          batchDate = eventBatchDate,
          projectPath = projectPath
        )

        findEvent(eventId) shouldBe Some((executionDate, Processing, None))

        (waitingEventsGauge.increment _).expects(projectPath).returning(IO.unit)
        (underProcessingGauge.decrement _).expects(projectPath).returning(IO.unit)

        val maybeMessage = Gen.option(eventMessages).generateOne
        val command =
          ToRecoverableFailure[IO](eventId, maybeMessage, waitingEventsGauge, underProcessingGauge, currentTime)

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

        findEvent(eventId) shouldBe Some((ExecutionDate(now.plus(10, MINUTES)), RecoverableFailure, maybeMessage))

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
                     batchDate = eventBatchDate
          )

          findEvent(eventId) shouldBe Some((executionDate, eventStatus, None))

          val maybeMessage = Gen.option(eventMessages).generateOne
          val command =
            ToRecoverableFailure[IO](eventId, maybeMessage, waitingEventsGauge, underProcessingGauge, currentTime)

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Conflict

          findEvent(eventId) shouldBe Some((executionDate, eventStatus, None))

          histogram.verifyExecutionTimeMeasured(command.query.name)
        }
    }
  }

  private trait TestCase {
    val waitingEventsGauge   = mock[LabeledGauge[IO, projects.Path]]
    val underProcessingGauge = mock[LabeledGauge[IO, projects.Path]]
    val histogram            = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val currentTime          = mockFunction[Instant]
    val eventId              = compoundEventIds.generateOne
    val eventBatchDate       = batchDates.generateOne

    val commandRunner = new StatusUpdatesRunnerImpl(transactor, histogram, TestLogger[IO]())

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()
  }

  private def findEvent(eventId: CompoundEventId): Option[(ExecutionDate, EventStatus, Option[EventMessage])] =
    execute {
      sql"""select execution_date, status, message
           |from event_log 
           |where event_id = ${eventId.id} and project_id = ${eventId.projectId}
         """.stripMargin
        .query[(ExecutionDate, EventStatus, Option[EventMessage])]
        .option
    }
}
