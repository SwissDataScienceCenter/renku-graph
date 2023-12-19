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

import Generators.sendingResults
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators.subscriberUrls
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DispatchRecoverySpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  "returnToQueue" should {

    s"delete the row from the subscription_category_sync_times table for the $categoryName and project" in testDBResource
      .use { implicit cfg =>
        for {
          _ <- upsertProject(project)
          _ <- upsertCategorySyncTime(event.projectId, categoryName)

          _ <- findCategorySyncTimes(event.projectId).asserting(_.map(_.name) shouldBe List(categoryName))

          _ <- recovery.returnToQueue(event, sendingResults.generateOne).assertNoException

          _ <- findCategorySyncTimes(event.projectId).asserting(_ shouldBe Nil)
        } yield Succeeded
      }

    s"do nothing if there's no row in the subscription_category_sync_times table for the $categoryName and project" in testDBResource
      .use { implicit cfg =>
        for {
          _ <- findCategorySyncTimes(event.projectId).asserting(_ shouldBe Nil)
          _ <- recovery.returnToQueue(event, sendingResults.generateOne).assertNoException
          _ <- findCategorySyncTimes(event.projectId).asserting(_ shouldBe Nil)
        } yield Succeeded
      }
  }

  "recover" should {

    val exception  = exceptions.generateOne
    val subscriber = subscriberUrls.generateOne

    s"delete the row from the subscription_category_sync_times table for the $categoryName and project" in testDBResource
      .use { implicit cfg =>
        for {
          _ <- upsertProject(project)
          _ <- upsertCategorySyncTime(event.projectId, categoryName)

          _ <- findCategorySyncTimes(event.projectId).asserting(_.map(_.name) shouldBe List(categoryName))

          _ <- recovery.recover(subscriber, event)(exception).assertNoException

          _ <- findCategorySyncTimes(event.projectId).asserting(_ shouldBe Nil)
        } yield Succeeded
      }

    s"do nothing if there's no row in the subscription_category_sync_times table for the $categoryName and project" in testDBResource
      .use { implicit cfg =>
        findCategorySyncTimes(event.projectId).asserting(_ shouldBe Nil) >>
          recovery.recover(subscriber, event)(exception).assertNoException >>
          findCategorySyncTimes(event.projectId).asserting(_ shouldBe Nil)
      }
  }

  private val project = consumerProjects.generateOne
  private val event   = ProjectSyncEvent(project.id, project.slug)

  private def recovery(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DispatchRecoveryImpl[IO]
  }
}
