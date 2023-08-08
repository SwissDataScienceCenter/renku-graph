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

package io.renku.eventlog.events.consumers.commitsyncrequest

import cats.effect.IO
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.eventlog.events.producers._
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventDate
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CommitSyncForcerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with TypeSerializers
    with should.Matchers {

  "forceCommitSync" should {

    "remove row for the given project id and COMMIT_SYNC category " +
      "from the subscription_category_sync_time " +
      "if it exists" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectSlug = projectSlugs.generateOne
        upsertProject(projectId, projectSlug, eventDates.generateOne)

        val otherCategoryName = categoryNames.generateOne
        upsertCategorySyncTime(projectId, commitsync.categoryName, lastSyncedDates.generateOne)
        upsertCategorySyncTime(projectId, otherCategoryName, lastSyncedDates.generateOne)

        findSyncTime(projectId, commitsync.categoryName) shouldBe a[Some[_]]
        findSyncTime(projectId, otherCategoryName)       shouldBe a[Some[_]]

        forcer.forceCommitSync(projectId, projectSlug).unsafeRunSync() shouldBe ()

        findSyncTime(projectId, commitsync.categoryName) shouldBe None
        findSyncTime(projectId, otherCategoryName)       shouldBe a[Some[_]]
      }

    "upsert a new project " +
      "if there's no row the given project id and category in the subscription_category_sync_time" +
      "and there's no project in the project table" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectSlug = projectSlugs.generateOne

        findSyncTime(projectId, commitsync.categoryName) shouldBe None

        forcer.forceCommitSync(projectId, projectSlug).unsafeRunSync() shouldBe ()

        findSyncTime(projectId, commitsync.categoryName) shouldBe None
        findProjects shouldBe List((projectId, projectSlug, EventDate(Instant.EPOCH)))
      }

    "do nothing " +
      "if there's no row the given project id and category in the subscription_category_sync_time" +
      "but the project exists in the project table" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectSlug = projectSlugs.generateOne

        upsertProject(projectId, projectSlug, eventDates.generateOne)
        findProjects.map(proj => proj._1 -> proj._2) shouldBe List(projectId -> projectSlug)

        findSyncTime(projectId, commitsync.categoryName) shouldBe None

        forcer.forceCommitSync(projectId, projectSlug).unsafeRunSync() shouldBe ()

        findSyncTime(projectId, commitsync.categoryName) shouldBe None
        findProjects.map(proj => proj._1 -> proj._2)     shouldBe List(projectId -> projectSlug)
      }
  }

  private trait TestCase {
    private implicit val metricsRegistry: TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    implicit val queriesExecTimes:        QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val forcer = new CommitSyncForcerImpl[IO]
  }
}
