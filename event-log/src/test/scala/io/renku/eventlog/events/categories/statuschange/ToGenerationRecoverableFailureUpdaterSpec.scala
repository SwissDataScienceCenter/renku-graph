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
import ch.datascience.graph.model.EventsGenerators.{eventBodies, eventIds}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventStatus}
import EventStatus._
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog._
import EventContentGenerators.eventMessages
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToGenerationRecoverableFailure
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ToGenerationRecoverableFailureUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    s"change the status of the given event from $GeneratingTriples to $GenerationRecoverableFailure" in new TestCase {

      val eventId      = addEvent(GeneratingTriples)
      val otherEventId = addEvent(GeneratingTriples)

      val statusChangeEvent =
        ToGenerationRecoverableFailure(CompoundEventId(eventId, projectId), projectPath, eventMessages.generateOne)

      sessionResource
        .useK(dbUpdater.updateDB(statusChangeEvent))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        projectPath,
        Map(GeneratingTriples -> -1, GenerationRecoverableFailure -> 1)
      )

      val updatedEvent = findEvent(CompoundEventId(eventId, projectId))
      updatedEvent.map(_._2)                                        shouldBe Some(GenerationRecoverableFailure)
      updatedEvent.flatMap(_._3)                                    shouldBe Some(statusChangeEvent.message)
      findEvent(CompoundEventId(otherEventId, projectId)).map(_._2) shouldBe Some(GeneratingTriples)
    }

    s"fail if the given event is not in the $GeneratingTriples status" in new TestCase {

      val invalidStatus = Gen.oneOf(EventStatus.all.filterNot(_ == GeneratingTriples)).generateOne
      val eventId       = addEvent(invalidStatus)

      intercept[Exception] {
        sessionResource
          .useK(
            dbUpdater.updateDB(
              ToGenerationRecoverableFailure(CompoundEventId(eventId, projectId),
                                             projectPath,
                                             eventMessages.generateOne
              )
            )
          )
          .unsafeRunSync()
      }.getMessage shouldBe s"Could not update event ${CompoundEventId(eventId, projectId)} to status $GenerationRecoverableFailure"

      findEvent(CompoundEventId(eventId, projectId)).map(_._2) shouldBe Some(invalidStatus)
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new ToGenerationRecoverableFailureUpdater[IO](queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status: EventStatus): EventId = {
      val eventId = CompoundEventId(eventIds.generateOne, projectId)

      storeEvent(
        eventId,
        status,
        timestampsNotInTheFuture.generateAs(ExecutionDate),
        timestampsNotInTheFuture.generateAs(EventDate),
        eventBodies.generateOne,
        projectPath = projectPath
      )
      eventId.id
    }
  }
}
