/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange.toawaitingdeletion

import cats.effect.IO
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults
import io.renku.eventlog.api.events.StatusChangeEvent.ToAwaitingDeletion
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.events.consumers.ConsumersModelGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventsGenerators.{eventBodies, eventIds, eventStatuses}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class DbUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers
    with MockFactory {

  "updateDB" should {

    s"change status of the given event to $AwaitingDeletion" in new TestCase {

      val eventOldStatus = eventStatuses.generateOne
      val eventId        = addEvent(eventOldStatus)

      sessionResource
        .useK(dbUpdater updateDB ToAwaitingDeletion(eventId, project))
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects(
        project.slug,
        Map(eventOldStatus -> -1, AwaitingDeletion -> 1)
      )

      findEvent(CompoundEventId(eventId, project.id)).map(_._2) shouldBe Some(AwaitingDeletion)
    }

    "do nothing if there's no event specified in the event" in new TestCase {
      sessionResource
        .useK(
          dbUpdater updateDB ToAwaitingDeletion(
            eventIds.generateOne,
            ConsumersModelGenerators.consumerProjects.generateOne
          )
        )
        .unsafeRunSync() shouldBe DBUpdateResults.ForProjects.empty
    }
  }

  private trait TestCase {

    val project = ConsumersModelGenerators.consumerProjects.generateOne

    val currentTime = mockFunction[Instant]
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val dbUpdater = new DbUpdater[IO](currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(status: EventStatus): EventId = {
      val eventId = CompoundEventId(eventIds.generateOne, project.id)

      storeEvent(
        eventId,
        status,
        timestampsNotInTheFuture.generateAs(ExecutionDate),
        timestampsNotInTheFuture.generateAs(EventDate),
        eventBodies.generateOne,
        projectSlug = project.slug
      )
      eventId.id
    }
  }
}
