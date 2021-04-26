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

package io.renku.eventlog.eventspatching

import cats.data.NonEmptyList

import java.time.Instant
import cats.effect.IO
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects.Path
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class StatusNewPatchSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "StatusNewPatch" should {

    s"set status to $New, batch_date to the current time, execution_date to event_date and clean-up the message on all events" in new TestCase {

      val event1Id   = compoundEventIds.generateOne
      val event1Date = eventDates.generateOne
      addEvent(event1Id, EventStatus.GeneratingTriples, timestampsNotInTheFuture.map(ExecutionDate.apply), event1Date)
      val event2Id   = compoundEventIds.generateOne
      val event2Date = eventDates.generateOne
      addEvent(event2Id, EventStatus.GeneratingTriples, timestampsInTheFuture.map(ExecutionDate.apply), event2Date)
      val event3Id   = compoundEventIds.generateOne
      val event3Date = eventDates.generateOne
      addEvent(event3Id, EventStatus.TriplesStore, timestampsNotInTheFuture.map(ExecutionDate.apply), event3Date)
      val event4Id      = compoundEventIds.generateOne
      val event4Date    = eventDates.generateOne
      val event4Message = Some(eventMessages.generateOne)
      val event4ExecutionDate: Gen[ExecutionDate] = timestampsNotInTheFuture.map(ExecutionDate.apply)
      addEvent(event4Id, GenerationNonRecoverableFailure, event4ExecutionDate, event4Date, event4Message)
      val event5Id   = compoundEventIds.generateOne
      val event5Date = eventDates.generateOne
      addEvent(event5Id, EventStatus.New, timestampsNotInTheFuture.map(ExecutionDate.apply), event5Date)
      val event6Id   = compoundEventIds.generateOne
      val event6Date = eventDates.generateOne
      addEvent(event6Id, GenerationRecoverableFailure, timestampsNotInTheFuture.map(ExecutionDate.apply), event6Date)

      (waitingEventsGauge.reset _).expects().returning(IO.unit)
      (underProcessingGauge.reset _).expects().returning(IO.unit)

      patcher.applyToAllEvents(patch).unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = New).toSet shouldBe Set(
        (event1Id, ExecutionDate(event1Date.value), BatchDate(currentTime)),
        (event2Id, ExecutionDate(event2Date.value), BatchDate(currentTime)),
        (event3Id, ExecutionDate(event3Date.value), BatchDate(currentTime)),
        (event4Id, ExecutionDate(event4Date.value), BatchDate(currentTime)),
        (event5Id, ExecutionDate(event5Date.value), BatchDate(currentTime)),
        (event6Id, ExecutionDate(event6Date.value), BatchDate(currentTime))
      )
      findEventMessage(event4Id) shouldBe None

      queriesExecTimes.verifyExecutionTimeMeasured(
        NonEmptyList[SqlStatement.Name](Refined.unsafeApply(s"status $New patch"), Nil)
      )
    }
  }

  private trait TestCase {

    val waitingEventsGauge          = mock[LabeledGauge[IO, Path]]
    val underProcessingGauge        = mock[LabeledGauge[IO, Path]]
    val queriesExecTimes            = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val currentTime                 = Instant.now()
    private val currentTimeProvider = mockFunction[Instant]
    currentTimeProvider.expects().returning(currentTime)
    val patch = StatusNewPatch(waitingEventsGauge, underProcessingGauge, currentTimeProvider)

    val patcher = new EventsPatcherImpl(sessionResource, queriesExecTimes, TestLogger[IO]())

    def addEvent(commitEventId: CompoundEventId,
                 status:        EventStatus,
                 executionDate: Gen[ExecutionDate],
                 eventDate:     EventDate,
                 maybeMessage:  Option[EventMessage] = None
    ): Unit =
      storeEvent(commitEventId,
                 status,
                 executionDate.generateOne,
                 eventDate,
                 eventBodies.generateOne,
                 projectPath = projectPaths.generateOne,
                 maybeMessage = maybeMessage
      )
  }
}
