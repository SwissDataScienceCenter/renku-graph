/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.producers.SubscriptionProvisioning
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{CleanUpEventsProvisioning, EventLogDB, EventLogPostgresSpec}
import io.renku.events.CategoryName
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.LastSyncedDate
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ProjectRemoverSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with CleanUpEventsProvisioning
    with should.Matchers {

  private val project = consumerProjects.generateOne

  "removeProject" should {

    "delete project with the given project_id," +
      "remove all its events and corresponding payloads, processing times and delivery infos," +
      "remove category sync times and clean-up events" in testDBResource.use { implicit cfg =>
        val eventDate = eventDates.generateOne
        for {
          event <- storeGeneratedEvent(eventStatuses.generateOne, eventDate, project)

          _ <- upsertEventDeliveryInfo(event.eventId)
          _ <- upsertCategorySyncTime(project.id,
                                      nonEmptyStrings().generateAs(CategoryName),
                                      timestampsNotInTheFuture.generateAs(LastSyncedDate)
               )
          _ <- insertCleanUpEvent(project)

          _ <- remover.removeProject(project.id).assertNoException

          _ <- findProjects.asserting(_ shouldBe Nil)
          _ <- findAllProjectEvents(project.id).asserting(_ shouldBe Nil)
          _ <- findAllProjectPayloads(project.id).asserting(_ shouldBe Nil)
          _ <- findProcessingTimes(project.id).asserting(_ shouldBe Nil)
          _ <- findAllProjectDeliveries.asserting(_ shouldBe Nil)
          _ <- findCategorySyncTimes(project.id).asserting(_ shouldBe Nil)
          _ <- findCleanUpEvents.asserting(_ shouldBe Nil)
        } yield Succeeded
      }

    "do nothing if project with the given id does not exist" in testDBResource.use { implicit cfg =>
      remover.removeProject(project.id).assertNoException >>
        findProjects.asserting(_ shouldBe Nil)
    }
  }

  private def remover(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new ProjectRemoverImpl[IO]
  }
}
