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

package io.renku.eventlog.events.consumers.globalcommitsyncrequest

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.producers._
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.{EventDate, LastSyncedDate}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.temporal.ChronoUnit.MICROS
import java.time.{Duration, Instant}

class GlobalCommitSyncForcerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  "moveGlobalCommitSync" should {

    "schedule the next the GLOBAL_COMMIT_SYNC event for the project " +
      "in delayOnRequest (syncFrequence - delayOnRequest) " +
      "if the row in the subscription_category_sync_time exists" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne
        for {
          _ <- upsertProject(project)

          otherCategoryName = categoryNames.generateOne
          globalSyncSyncDate =
            timestampsNotInTheFuture(butYoungerThan = now.minus(syncFrequency minusDays 1)).generateAs(LastSyncedDate)
          _ <- upsertCategorySyncTime(project.id, globalcommitsync.categoryName, globalSyncSyncDate)
          otherCategorySyncDate = lastSyncedDates.generateOne
          _ <- upsertCategorySyncTime(project.id, otherCategoryName, otherCategorySyncDate)

          _ <- findSyncTime(project.id, globalcommitsync.categoryName).asserting(_ shouldBe globalSyncSyncDate.some)
          _ <- findSyncTime(project.id, otherCategoryName).asserting(_ shouldBe otherCategorySyncDate.some)

          _ <- forcer.moveGlobalCommitSync(project.id, project.slug).assertNoException

          _ <- findSyncTime(project.id, globalcommitsync.categoryName).asserting(
                 _ shouldBe LastSyncedDate(now.minus(syncFrequency).plus(delayOnRequest)).some
               )
          _ <- findSyncTime(project.id, otherCategoryName).asserting(_ shouldBe otherCategorySyncDate.some)
        } yield Succeeded
      }

    "upsert a new project " +
      "if there's no row the given project id and category in the subscription_category_sync_time " +
      "and there's no project in the project table" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne
        for {
          _ <- findSyncTime(project.id, globalcommitsync.categoryName).asserting(_ shouldBe None)

          _ <- forcer.moveGlobalCommitSync(project.id, project.slug).assertNoException

          _ <- findSyncTime(project.id, globalcommitsync.categoryName).asserting(_ shouldBe None)
          _ <- findProjects.asserting(_ shouldBe List(FoundProject(project, EventDate(Instant.EPOCH))))
        } yield Succeeded
      }

    "do nothing " +
      "if there's no row for the given project id and category in the subscription_category_sync_time" +
      "but the project exists in the project table" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne
        for {
          _ <- upsertProject(project)

          _ <- findSyncTime(project.id, globalcommitsync.categoryName).asserting(_ shouldBe None)

          _ <- forcer.moveGlobalCommitSync(project.id, project.slug).assertNoException

          _ <- findSyncTime(project.id, globalcommitsync.categoryName).asserting(_ shouldBe None)
          _ <- findProjects.asserting(_.map(_.project) shouldBe List(project))
        } yield Succeeded
      }
  }

  private val syncFrequency  = Duration ofDays 7
  private val delayOnRequest = Duration ofMinutes 5
  private val now            = Instant.now().truncatedTo(MICROS)
  private def forcer(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new GlobalCommitSyncForcerImpl[IO](syncFrequency, delayOnRequest, () => now)
  }
}
