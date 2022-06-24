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

package io.renku.eventlog.events.consumers.projectsync

import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog.subscriptions.SubscriptionDataProvisioning
import io.renku.eventlog.{CleanUpEventsProvisioning, InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.events.CategoryName
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.{CompoundEventId, LastSyncedDate}

import java.time.OffsetDateTime

class ProjectRemoverSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with CleanUpEventsProvisioning
    with TypeSerializers
    with should.Matchers {

  "removeProject" should {

    "delete project with the given project_id," +
      "remove all its events and corresponding payloads, processing times and delivery infos," +
      "remove category sync times and clean-up events" in new TestCase {

        val eventDate             = eventDates.generateOne
        val (eventId, _, _, _, _) = storeGeneratedEvent(eventStatuses.generateOne, eventDate, projectId, projectPath)
        upsertEventDeliveryInfo(CompoundEventId(eventId, projectId))
        upsertCategorySyncTime(projectId,
                               nonEmptyStrings().generateAs(CategoryName),
                               timestampsNotInTheFuture.generateAs(LastSyncedDate)
        )
        insertCleanUpEvent(projectId, projectPath, OffsetDateTime.now())

        remover.removeProject(projectId).unsafeRunSync() shouldBe ()

        findProjects                            shouldBe Nil
        findAllProjectEvents(projectId)         shouldBe Nil
        findAllProjectPayloads(projectId)       shouldBe Nil
        findProjectProcessingTimes(projectId)   shouldBe Nil
        findAllProjectDeliveries                shouldBe Nil
        findProjectCategorySyncTimes(projectId) shouldBe Nil
        findCleanUpEvents                       shouldBe Nil
      }

    "do nothing if project with the given id does not exist" in new TestCase {

      remover.removeProject(projectId).unsafeRunSync() shouldBe ()

      findProjects shouldBe Nil
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val remover          = new ProjectRemoverImpl[IO](queriesExecTimes)
  }
}
