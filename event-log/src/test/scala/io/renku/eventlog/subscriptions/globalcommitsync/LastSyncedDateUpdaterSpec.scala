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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlStatement.Name
import ch.datascience.events.consumers.ConsumersModelGenerators.projectsGen
import io.renku.eventlog.EventContentGenerators.eventDates
import ch.datascience.events.consumers.Project
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.lastSyncedDates
import ch.datascience.graph.model.events.{CategoryName, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.SubscriptionDataProvisioning
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.data.Completion
import skunk.implicits.{toIdOps, toStringOps}
import skunk.{Query, ~}

class LastSyncedDateUpdaterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "run" should {
    "delete the current last synced date if the argument is None" in new TestCase {
      val oldLastSyncedDate = lastSyncedDates.generateOne
      upsertLastSynced(project.id, categoryName, oldLastSyncedDate)

      getLastSyncedDate(project) shouldBe oldLastSyncedDate.some

      updater.run(project.id, None).unsafeRunSync() shouldBe Completion.Delete(1)

      getLastSyncedDate(project) shouldBe None

    }

    "update the previous last synced date" in new TestCase {
      val oldLastSyncedDate = lastSyncedDates.generateOne
      upsertLastSynced(project.id, categoryName, oldLastSyncedDate)

      getLastSyncedDate(project) shouldBe oldLastSyncedDate.some

      val newLastSyncedDate = lastSyncedDates.generateOne

      updater
        .run(project.id, newLastSyncedDate.some)
        .unsafeRunSync() shouldBe Completion.Insert(1)

      getLastSyncedDate(project) shouldBe newLastSyncedDate.some
    }

    "insert a new last synced date" in new TestCase {
      val newLastSyncedDate = lastSyncedDates.generateOne

      updater
        .run(project.id, newLastSyncedDate.some)
        .unsafeRunSync() shouldBe Completion.Insert(1)

      getLastSyncedDate(project) shouldBe newLastSyncedDate.some
    }
  }

  private trait TestCase {
    val project = projectsGen.generateOne
    val updater =
      new LastSyncedDateUpdateImpl[IO](sessionResource, TestLabeledHistogram[Name]("query_id"))

    upsertProject(project.id, project.path, eventDates.generateOne)
  }

  private def getLastSyncedDate(project: Project): Option[LastSyncedDate] =
    execute {
      Kleisli { session =>
        val query: Query[projects.Id ~ CategoryName, LastSyncedDate] =
          sql"""SELECT sync_time.last_synced FROM subscription_category_sync_time sync_time
                                WHERE sync_time.project_id = $projectIdEncoder 
                                AND sync_time.category_name = $categoryNameEncoder
                                """
            .query(lastSyncedDateDecoder)

        session.prepare(query).use(p => p.option(project.id ~ categoryName))
      }
    }
}
