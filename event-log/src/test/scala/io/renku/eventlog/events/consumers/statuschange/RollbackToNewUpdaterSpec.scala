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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventsGenerators.{eventBodies, eventIds}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events._
import EventStatus._
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent.RollbackToNew
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class RollbackToNewUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    s"change the status of the given event from $GeneratingTriples to $New" in new TestCase {

      val eventId      = addEvent(GeneratingTriples)
      val otherEventId = addEvent(GeneratingTriples)

      sessionResource
        .useK(dbUpdater.updateDB(RollbackToNew(CompoundEventId(eventId, projectId), projectPath)))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        projectPath,
        Map(GeneratingTriples -> -1, New -> 1)
      )

      findEvent(CompoundEventId(eventId, projectId)).map(_._2)      shouldBe Some(New)
      findEvent(CompoundEventId(otherEventId, projectId)).map(_._2) shouldBe Some(GeneratingTriples)
    }

    s"do nothing if event is not in the $GeneratingTriples status" in new TestCase {

      val invalidStatus = Gen.oneOf(EventStatus.all.filterNot(_ == GeneratingTriples)).generateOne
      val eventId       = addEvent(invalidStatus)

      sessionResource
        .useK(dbUpdater.updateDB(RollbackToNew(CompoundEventId(eventId, projectId), projectPath)))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty

      findEvent(CompoundEventId(eventId, projectId)).map(_._2) shouldBe Some(invalidStatus)
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new RollbackToNewUpdater[IO](queriesExecTimes, currentTime)

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
