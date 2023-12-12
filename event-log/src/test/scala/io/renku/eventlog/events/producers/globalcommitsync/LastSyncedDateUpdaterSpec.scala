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
package globalcommitsync

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.CategoryName
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.EventsGenerators.lastSyncedDates
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.data.Completion
import skunk.implicits.toStringOps

class LastSyncedDateUpdaterSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with SubscriptionProvisioning
    with AsyncMockFactory
    with should.Matchers {

  private val project = consumerProjects.generateOne

  it should "delete the current last synced date if the argument is None" in testDBResource.use { implicit cfg =>
    val project           = consumerProjects.generateOne
    val oldLastSyncedDate = lastSyncedDates.generateOne

    for {
      _ <- upsertProject(project, eventDates.generateOne)
      _ <- upsertCategorySyncTime(project.id, categoryName, oldLastSyncedDate)

      _ <- getLastSyncedDate(project).asserting(_ shouldBe oldLastSyncedDate.some)

      _ <- updater.run(project.id, None).asserting(_ shouldBe Completion.Delete(1))

      _ <- getLastSyncedDate(project).asserting(_ shouldBe None)
    } yield Succeeded
  }

  it should "update the previous last synced date" in testDBResource.use { implicit cfg =>
    val oldLastSyncedDate = lastSyncedDates.generateOne
    for {
      _ <- upsertProject(project, eventDates.generateOne)
      _ <- upsertCategorySyncTime(project.id, categoryName, oldLastSyncedDate)

      _ <- getLastSyncedDate(project).asserting(_ shouldBe oldLastSyncedDate.some)

      newLastSyncedDate = lastSyncedDates.generateOne

      _ <- updater.run(project.id, newLastSyncedDate.some).asserting(_ shouldBe Completion.Insert(1))

      _ <- getLastSyncedDate(project).asserting(_ shouldBe newLastSyncedDate.some)
    } yield Succeeded
  }

  it should "insert a new last synced date" in testDBResource.use { implicit cfg =>
    val newLastSyncedDate = lastSyncedDates.generateOne

    for {
      _ <- upsertProject(project, eventDates.generateOne)

      _ <- updater.run(project.id, newLastSyncedDate.some).asserting(_ shouldBe Completion.Insert(1))

      _ <- getLastSyncedDate(project).asserting(_ shouldBe newLastSyncedDate.some)
    } yield Succeeded
  }

  it should "do nothing if project with the given id does not exist" in testDBResource.use { implicit cfg =>
    upsertProject(project, eventDates.generateOne) >>
      updater
        .run(projectIds.generateOne, lastSyncedDates.generateSome)
        .asserting(_ shouldBe Completion.Insert(0))
  }

  private def updater(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new LastSyncedDateUpdateImpl[IO]
  }

  private def getLastSyncedDate(project: Project)(implicit cfg: DBConfig[EventLogDB]): IO[Option[LastSyncedDate]] =
    moduleSessionResource.session.use { session =>
      val query: Query[projects.GitLabId *: CategoryName *: EmptyTuple, LastSyncedDate] = sql"""
          SELECT sync_time.last_synced
          FROM subscription_category_sync_time sync_time
          WHERE sync_time.project_id = $projectIdEncoder
            AND sync_time.category_name = $categoryNameEncoder"""
        .query(lastSyncedDateDecoder)

      session.prepare(query).flatMap(p => p.option(project.id *: categoryName *: EmptyTuple))
    }
}
