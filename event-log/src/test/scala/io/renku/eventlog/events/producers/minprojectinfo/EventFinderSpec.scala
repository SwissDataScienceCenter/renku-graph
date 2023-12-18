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
package minprojectinfo

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus.TriplesStore
import io.renku.graph.model.events.{EventStatus, LastSyncedDate}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class EventFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  "popEvent" should {

    "return an event for the project with no events " +
      s"and no rows in the subscription_category_sync_times table for the $categoryName" in testDBResource.use {
        implicit cfg =>
          for {
            _ <- finder.popEvent().asserting(_ shouldBe None)

            project = consumerProjects.generateOne
            _ <- upsertProject(project)

            _ <- finder.popEvent().asserting(_ shouldBe Some(MinProjectInfoEvent(project.id, project.slug)))

            _ <- findCategorySyncTimes(project.id)
                   .asserting(_ shouldBe List(CategorySync(categoryName, LastSyncedDate(now))))

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield Succeeded
      }

    "return an event for the project with no events in TRIPLES_STORE " +
      s"and no rows in the subscription_category_sync_times table for the $categoryName" in testDBResource.use {
        implicit cfg =>
          val project = consumerProjects.generateOne
          for {
            _ <- upsertProject(project)
            _ <- (EventStatus.all - TriplesStore).toList.traverse_(storeGeneratedEvent(_, project = project))

            _ <- finder.popEvent().asserting(_ shouldBe Some(MinProjectInfoEvent(project.id, project.slug)))

            _ <- findCategorySyncTimes(project.id)
                   .asserting(_ shouldBe List(CategorySync(categoryName, LastSyncedDate(now.truncatedTo(MICROS)))))

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield Succeeded
      }

    "return no event for the project having at least on event in TRIPLES_STORE " +
      s"even there's no rows in the subscription_category_sync_times table for the $categoryName" in testDBResource
        .use { implicit cfg =>
          val project = consumerProjects.generateOne
          for {
            _ <- upsertProject(project)
            _ <- EventStatus.all.toList.traverse_(storeGeneratedEvent(_, project = project))
            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield Succeeded
        }
  }

  private lazy val now = Instant.now().truncatedTo(MICROS)
  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventFinderImpl[IO](() => now)
  }
}
