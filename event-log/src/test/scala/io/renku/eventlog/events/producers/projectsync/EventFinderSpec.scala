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

package io.renku.eventlog.events.producers
package projectsync

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.events.LastSyncedDate
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.temporal.ChronoUnit.MICROS
import java.time.{Duration, Instant}

class EventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  it should s"return an event for a project that has no row with the $categoryName in the subscription_category_sync_times table" in testDBResource
    .use { implicit cfg =>
      for {
        _ <- finder.popEvent().asserting(_ shouldBe None)

        project = consumerProjects.generateOne
        _ <- upsertProject(project)

        _ <- findCategorySyncTimes(project.id).asserting(_ shouldBe Nil)

        _ <- finder.popEvent().asserting(_ shouldBe Some(ProjectSyncEvent(project.id, project.slug)))

        _ <- findCategorySyncTimes(project.id)
               .asserting(_ shouldBe List(CategorySync(categoryName, LastSyncedDate(now))))

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should s"return an event for a project that has a row with the $categoryName in the subscription_category_sync_times table " +
    "with the last_synced > 24h " in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne
      for {
        _ <- upsertProject(project)

        lastSyncDate = relativeTimestamps(moreThanAgo = Duration.ofMinutes(24 * 60 + 1)).generateAs(LastSyncedDate)
        _ <- upsertCategorySyncTime(project.id, categoryName, lastSyncDate)

        _ <- findCategorySyncTimes(project.id).asserting(_ shouldBe List(CategorySync(categoryName, lastSyncDate)))

        _ <- finder.popEvent().asserting(_ shouldBe Some(ProjectSyncEvent(project.id, project.slug)))

        _ <- findCategorySyncTimes(project.id)
               .asserting(_ shouldBe List(CategorySync(categoryName, LastSyncedDate(now))))

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return events ordered by the subscription_category_sync_times's last_synced > 24h " +
    "where the projects with no rows in the table should go first" in testDBResource.use { implicit cfg =>
      val project1 = consumerProjects.generateOne
      for {
        _ <- upsertProject(project1)

        project2 = consumerProjects.generateOne
        _ <- upsertProject(project2)
        project2lastSyncDate = relativeTimestamps(moreThanAgo = Duration.ofDays(7)).generateAs(LastSyncedDate)
        _ <- upsertCategorySyncTime(project2.id, categoryName, project2lastSyncDate)

        project3 = consumerProjects.generateOne
        _ <- upsertProject(project3)
        project3lastSyncDate =
          timestampsNotInTheFuture(butYoungerThan = project2lastSyncDate.value).generateAs(LastSyncedDate)
        _ <- upsertCategorySyncTime(project3.id, categoryName, project3lastSyncDate)

        _ <- finder.popEvent().asserting(_ shouldBe Some(ProjectSyncEvent(project1.id, project1.slug)))
        _ <- finder.popEvent().asserting(_ shouldBe Some(ProjectSyncEvent(project2.id, project2.slug)))
        _ <- finder.popEvent().asserting(_ shouldBe Some(ProjectSyncEvent(project3.id, project3.slug)))
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  private lazy val now = Instant.now().truncatedTo(MICROS)
  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventFinderImpl[IO](() => now)
  }
}
