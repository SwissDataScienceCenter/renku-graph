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

package io.renku.eventlog.subscriptions
package minprojectinfo

import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.EventStatus.TriplesStore
import io.renku.graph.model.events.{EventStatus, LastSyncedDate}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return an event for the project with no events " +
      s"and no rows in the subscription_category_sync_times table for the $categoryName" in new TestCase {

        finder.popEvent().unsafeRunSync() shouldBe None

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne
        upsertProject(projectId, projectPath, eventDates.generateOne)

        finder.popEvent().unsafeRunSync() shouldBe Some(MinProjectInfoEvent(projectId, projectPath))

        findProjectCategorySyncTimes(projectId) shouldBe List(
          categoryName -> LastSyncedDate(currentTime.truncatedTo(MICROS))
        )

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event for the project with no events in TRIPLES_STORE " +
      s"and no rows in the subscription_category_sync_times table for the $categoryName" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne
        upsertProject(projectId, projectPath, eventDates.generateOne)

        EventStatus.all - TriplesStore foreach {
          storeGeneratedEvent(_, eventDates.generateOne, projectId, projectPath)
        }

        finder.popEvent().unsafeRunSync() shouldBe Some(MinProjectInfoEvent(projectId, projectPath))

        findProjectCategorySyncTimes(projectId) shouldBe List(
          categoryName -> LastSyncedDate(currentTime.truncatedTo(MICROS))
        )

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no event for the project having at least on event in TRIPLES_STORE " +
      s"even there's no rows in the subscription_category_sync_times table for the $categoryName" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne
        upsertProject(projectId, projectPath, eventDates.generateOne)

        EventStatus.all foreach {
          storeGeneratedEvent(_, eventDates.generateOne, projectId, projectPath)
        }

        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    val currentTime = Instant.now()
    val now         = mockFunction[Instant]
    now.expects().returning(currentTime).anyNumberOfTimes()
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val finder           = new EventFinderImpl(queriesExecTimes, now)
  }
}
