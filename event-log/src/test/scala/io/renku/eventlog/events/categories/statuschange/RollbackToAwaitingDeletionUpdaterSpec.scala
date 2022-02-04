/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventsGenerators.{eventBodies, eventIds, eventStatuses}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import EventStatus._
import eu.timepit.refined.auto._
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.RollbackToAwaitingDeletion
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import io.renku.events.consumers.Project

class RollbackToAwaitingDeletionUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    s"change status of all the event in the $Deleting status of a given project to $AwaitingDeletion" in new TestCase {
      val otherStatus = eventStatuses.filter(_ != Deleting).generateOne
      val event1Id    = addEvent(Deleting)
      val event2Id    = addEvent(otherStatus)
      val event3Id    = addEvent(Deleting)
      sessionResource
        .useK(dbUpdater updateDB RollbackToAwaitingDeletion(Project(projectId, projectPath)))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        projectPath,
        Map(Deleting -> -2, AwaitingDeletion -> 2)
      )

      findEvent(CompoundEventId(event1Id, projectId)).map(_._2) shouldBe Some(AwaitingDeletion)
      findEvent(CompoundEventId(event2Id, projectId)).map(_._2) shouldBe Some(otherStatus)
      findEvent(CompoundEventId(event3Id, projectId)).map(_._2) shouldBe Some(AwaitingDeletion)
    }

  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new RollbackToAwaitingDeletionUpdater[IO](queriesExecTimes, currentTime)

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
