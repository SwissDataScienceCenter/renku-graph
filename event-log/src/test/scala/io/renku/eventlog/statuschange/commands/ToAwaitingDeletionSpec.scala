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

package io.renku.eventlog.statuschange.commands

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies, eventProcessingTimes, eventStatuses}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.statuschange.ChangeStatusRequest.EventOnlyRequest
import io.renku.eventlog.statuschange.CommandFindingResult.{CommandFound, NotSupported}
import io.renku.eventlog.statuschange.StatusUpdatesRunnerImpl
import io.renku.eventlog.statuschange.commands.Generators._
import io.renku.eventlog.{EventPayload, ExecutionDate, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToAwaitingDeletionSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "command" should {

    EventStatus.all.filterNot(_ == AwaitingDeletion).foreach { currentStatus =>
      s"set status $AwaitingDeletion on the event with the given id and $currentStatus status, " +
        "decrement the corresponding event gauge  for the project, insert the processingTime " +
        s"and return ${UpdateResult.Updated}" in new TestCase {
          val projectPath = projectPaths.generateOne
          storeEvent(
            eventId,
            currentStatus,
            executionDates.generateOne,
            eventDates.generateOne,
            eventBodies.generateOne,
            batchDate = eventBatchDate,
            projectPath = projectPath
          )
          storeEvent(
            compoundEventIds.generateOne.copy(id = eventId.id),
            currentStatus,
            executionDates.generateOne,
            eventDates.generateOne,
            eventBodies.generateOne,
            batchDate = eventBatchDate
          )
          storeEvent(
            compoundEventIds.generateOne,
            currentStatus,
            executionDates.generateOne,
            eventDates.generateOne,
            eventBodies.generateOne,
            batchDate = eventBatchDate
          )

          findEvents(status = AwaitingDeletion) shouldBe List.empty

          getGaugeToDecrease(currentStatus).map(gauge => (gauge.decrement _).expects(projectPath).returning(IO.unit))

          val command =
            ToAwaitingDeletion[IO](
              eventId,
              currentStatus,
              awaitingTriplesGenerationGauge,
              underTriplesGenerationGauge,
              awaitingTriplesTransformationGauge,
              underTriplesTransformationGauge,
              processingTime,
              currentTime
            )

          (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

          findEvents(status = AwaitingDeletion)    shouldBe List((eventId, ExecutionDate(now), eventBatchDate))
          findProcessingTime(eventId).eventIdsOnly shouldBe List(eventId)

          histogram.verifyExecutionTimeMeasured(command.queries.map(_.name))
        }
    }

    "factory" should {

      "return a CommandFound when properly decoding a request" in new TestCase {
        val currentEventStatus = eventStatuses.generateOne
        createEvent(currentEventStatus)
        val maybeProcessingTime = eventProcessingTimes.generateOption
        val maybeMessage        = eventMessages.generateOption

        val actual = ToAwaitingDeletion
          .factory[IO](sessionResource,
                       awaitingTriplesGenerationGauge,
                       underTriplesGenerationGauge,
                       awaitingTriplesTransformationGauge,
                       underTriplesTransformationGauge
          )
          .run(EventOnlyRequest(eventId, AwaitingDeletion, maybeProcessingTime, maybeMessage))
          .unsafeRunSync()

        actual shouldBe CommandFound(
          ToAwaitingDeletion(
            eventId,
            currentEventStatus,
            awaitingTriplesGenerationGauge,
            underTriplesGenerationGauge,
            awaitingTriplesTransformationGauge,
            underTriplesTransformationGauge,
            maybeProcessingTime
          )
        )
      }

      EventStatus.all.filterNot(status => status == New) foreach { eventStatus =>
        s"return None if the decoding failed with status: $eventStatus " in new TestCase {
          ToNew
            .factory[IO](awaitingTriplesGenerationGauge, underTriplesGenerationGauge)
            .run(changeStatusRequestsWith(eventStatus).generateOne)
            .unsafeRunSync() shouldBe NotSupported
        }
      }
    }
  }

  private trait TestCase {
    val awaitingTriplesGenerationGauge     = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesGenerationGauge        = mock[LabeledGauge[IO, projects.Path]]
    val awaitingTriplesTransformationGauge = mock[LabeledGauge[IO, projects.Path]]
    val underTriplesTransformationGauge    = mock[LabeledGauge[IO, projects.Path]]
    val histogram                          = TestLabeledHistogram[SqlStatement.Name]("query_id")

    val currentTime    = mockFunction[Instant]
    val eventId        = compoundEventIds.generateOne
    val eventBatchDate = batchDates.generateOne
    val processingTime = eventProcessingTimes.generateSome

    val commandRunner = new StatusUpdatesRunnerImpl(sessionResource, histogram, TestLogger[IO]())

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def createEvent(status: EventStatus, maybePayload: Option[EventPayload] = None): Unit = storeEvent(
      eventId,
      status,
      executionDates.generateOne,
      eventDates.generateOne,
      eventBodies.generateOne,
      batchDate = eventBatchDate,
      maybeEventPayload = maybePayload
    )

    lazy val getGaugeToDecrease: EventStatus => Option[LabeledGauge[IO, projects.Path]] = {
      case New                 => awaitingTriplesGenerationGauge.some
      case GeneratingTriples   => underTriplesGenerationGauge.some
      case TriplesGenerated    => awaitingTriplesTransformationGauge.some
      case TransformingTriples => underTriplesTransformationGauge.some
      case _                   => None
    }
  }
}
