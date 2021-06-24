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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.IO
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.timestampsNotInTheFuture
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventIds}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import EventStatus._
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog._
import EventContentGenerators.eventMessages
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.{AllowedCombination, ToFailure}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToFailureUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    "change status of the given event from ProcessingStatus to FailureStatus" in new TestCase {
      Set(
        createFailureEvent(GeneratingTriples, GenerationNonRecoverableFailure),
        createFailureEvent(GeneratingTriples, GenerationRecoverableFailure),
        createFailureEvent(TransformingTriples, TransformationNonRecoverableFailure),
        createFailureEvent(TransformingTriples, TransformationRecoverableFailure)
      ) foreach { statusChangeEvent =>
        sessionResource
          .useK(dbUpdater updateDB statusChangeEvent)
          .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
          projectPath,
          Map(statusChangeEvent.currentStatus -> -1, statusChangeEvent.newStatus -> 1)
        )

        val updatedEvent = findEvent(statusChangeEvent.eventId)
        updatedEvent.map(_._2)     shouldBe Some(statusChangeEvent.newStatus)
        updatedEvent.flatMap(_._3) shouldBe Some(statusChangeEvent.message)
      }
    }

    EventStatus.all.diff(Set(GeneratingTriples, TransformingTriples)) foreach { invalidStatus =>
      s"fail if the given event is in $invalidStatus" in new TestCase {

        val eventId = addEvent(compoundEventIds.generateOne.copy(projectId = projectId), invalidStatus)
        val message = eventMessages.generateOne

        Set(
          ToFailure(eventId, projectPath, message, GeneratingTriples, GenerationNonRecoverableFailure),
          ToFailure(eventId, projectPath, message, GeneratingTriples, GenerationRecoverableFailure),
          ToFailure(eventId, projectPath, message, TransformingTriples, TransformationNonRecoverableFailure),
          ToFailure(eventId, projectPath, message, TransformingTriples, TransformationRecoverableFailure)
        ) foreach { statusChangeEvent =>
          intercept[Exception] {
            sessionResource.useK(dbUpdater.updateDB(statusChangeEvent)).unsafeRunSync()
          }.getMessage shouldBe s"Could not update event $eventId to status ${statusChangeEvent.newStatus}"

          findEvent(eventId).map(_._2) shouldBe Some(invalidStatus)
        }
      }
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new ToFailureUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def createFailureEvent[C <: ProcessingStatus, N <: FailureStatus](currentStatus: C, newStatus: N)(implicit
        evidence:                                                                    AllowedCombination[C, N]
    ): ToFailure[C, N] =
      ToFailure(addEvent(currentStatus), projectPath, eventMessages.generateOne, currentStatus, newStatus)

    def addEvent[S <: ProcessingStatus](status: S): CompoundEventId = {
      val eventId = CompoundEventId(eventIds.generateOne, projectId)
      addEvent(eventId, status)
      eventId
    }

    def addEvent(eventId: CompoundEventId, status: EventStatus): CompoundEventId = {
      storeEvent(
        eventId,
        status,
        timestampsNotInTheFuture.generateAs(ExecutionDate),
        timestampsNotInTheFuture.generateAs(EventDate),
        eventBodies.generateOne,
        projectPath = projectPath
      )
      eventId
    }
  }
}
