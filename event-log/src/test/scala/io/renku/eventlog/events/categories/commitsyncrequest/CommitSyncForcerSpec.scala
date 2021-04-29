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

package io.renku.eventlog.events.categories.commitsyncrequest

import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommitSyncForcerSpec
    extends AnyWordSpec
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
        val projectPath = projectPaths.generateOne
        upsertProject(projectId, projectPath, eventDates.generateOne)

        val otherCategoryName = categoryNames.generateOne
        upsertLastSynced(projectId, commitsync.categoryName, lastSyncedDates.generateOne)
        upsertLastSynced(projectId, otherCategoryName, lastSyncedDates.generateOne)

        findSyncTime(projectId, commitsync.categoryName) shouldBe a[Some[_]]
        findSyncTime(projectId, otherCategoryName)       shouldBe a[Some[_]]

        forcer.forceCommitSync(projectId, projectPath).unsafeRunSync() shouldBe ()

        findSyncTime(projectId, commitsync.categoryName) shouldBe None
        findSyncTime(projectId, otherCategoryName)       shouldBe a[Some[_]]

        queriesExecTimes.verifyExecutionTimeMeasured("commit_sync_request - delete last_synced")
      }

    "upsert a new project " +
      "if there's no row the given project id and category in the subscription_category_sync_time" +
      "and there's no project in the project table" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        findSyncTime(projectId, commitsync.categoryName) shouldBe None

        forcer.forceCommitSync(projectId, projectPath).unsafeRunSync() shouldBe ()

        findSyncTime(projectId, commitsync.categoryName) shouldBe None
        findProjects.map(proj => proj._1 -> proj._2) shouldBe List(projectId -> projectPath)

        queriesExecTimes.verifyExecutionTimeMeasured("commit_sync_request - delete last_synced")
        queriesExecTimes.verifyExecutionTimeMeasured("commit_sync_request - insert project")
      }

    "do nothing " +
      "if there's no row the given project id and category in the subscription_category_sync_time" +
      "but the project exists in the project table" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        upsertProject(projectId, projectPath, eventDates.generateOne)
        findProjects.map(proj => proj._1 -> proj._2) shouldBe List(projectId -> projectPath)

        findSyncTime(projectId, commitsync.categoryName) shouldBe None

        forcer.forceCommitSync(projectId, projectPath).unsafeRunSync() shouldBe ()

        findSyncTime(projectId, commitsync.categoryName) shouldBe None
        findProjects.map(proj => proj._1 -> proj._2) shouldBe List(projectId -> projectPath)

        queriesExecTimes.verifyExecutionTimeMeasured("commit_sync_request - delete last_synced")
      }
  }

  private trait TestCase {
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val forcer           = new CommitSyncForcerImpl(sessionResource, queriesExecTimes)
  }
}
