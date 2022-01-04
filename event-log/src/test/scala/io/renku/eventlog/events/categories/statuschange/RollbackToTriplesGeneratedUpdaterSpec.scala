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

import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventsGenerators.{eventBodies, eventIds}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import EventStatus._
import eu.timepit.refined.auto._
import io.renku.eventlog._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.RollbackToTriplesGenerated
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class RollbackToTriplesGeneratedUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    s"change the status of the given event from $TransformingTriples to $TriplesGenerated" in new TestCase {

      val eventId      = addEvent(TransformingTriples)
      val otherEventId = addEvent(TransformingTriples)

      sessionResource
        .useK(dbUpdater updateDB RollbackToTriplesGenerated(CompoundEventId(eventId, projectId), projectPath))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        projectPath,
        Map(TransformingTriples -> -1, TriplesGenerated -> 1)
      )

      findEvent(CompoundEventId(eventId, projectId)).map(_._2)      shouldBe Some(TriplesGenerated)
      findEvent(CompoundEventId(otherEventId, projectId)).map(_._2) shouldBe Some(TransformingTriples)
    }

    s"fail if the given event is not in the $TransformingTriples status" in new TestCase {

      val invalidStatus = Gen.oneOf(EventStatus.all.filterNot(_ == TransformingTriples)).generateOne
      val eventId       = addEvent(invalidStatus)

      intercept[Exception] {
        sessionResource
          .useK(dbUpdater updateDB RollbackToTriplesGenerated(CompoundEventId(eventId, projectId), projectPath))
          .unsafeRunSync()
      }.getMessage shouldBe s"Could not rollback event ${CompoundEventId(eventId, projectId)} to status $TriplesGenerated"

      findEvent(CompoundEventId(eventId, projectId)).map(_._2) shouldBe Some(invalidStatus)
    }
  }

  private trait TestCase {

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val currentTime      = mockFunction[Instant]
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val dbUpdater        = new RollbackToTriplesGeneratedUpdater(queriesExecTimes, currentTime)

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
