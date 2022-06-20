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
package projectsync

import Generators.sendingResults
import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.LastSyncedDate
import io.renku.interpreters.TestLogger
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DispatchRecoverySpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with should.Matchers
    with SubscriptionDataProvisioning {

  "returnToQueue" should {

    s"delete the row from the subscription_category_sync_times table for the $categoryName and project" in new TestCase {

      upsertProject(event.projectId, event.projectPath, eventDates.generateOne)
      upsertCategorySyncTime(event.projectId, categoryName, timestampsNotInTheFuture.generateAs(LastSyncedDate))

      findProjectCategorySyncTimes(event.projectId).map(_._1) shouldBe List(categoryName)

      recovery.returnToQueue(event, sendingResults.generateOne).unsafeRunSync() shouldBe ()

      findProjectCategorySyncTimes(event.projectId) shouldBe Nil
    }

    s"do nothing if there's no row in the subscription_category_sync_times table for the $categoryName and project" in new TestCase {

      findProjectCategorySyncTimes(event.projectId) shouldBe Nil

      recovery.returnToQueue(event, sendingResults.generateOne).unsafeRunSync() shouldBe ()

      findProjectCategorySyncTimes(event.projectId) shouldBe Nil
    }
  }

  "recover" should {

    val exception  = exceptions.generateOne
    val subscriber = subscriberUrls.generateOne

    s"delete the row from the subscription_category_sync_times table for the $categoryName and project" in new TestCase {

      upsertProject(event.projectId, event.projectPath, eventDates.generateOne)
      upsertCategorySyncTime(event.projectId, categoryName, timestampsNotInTheFuture.generateAs(LastSyncedDate))

      findProjectCategorySyncTimes(event.projectId).map(_._1) shouldBe List(categoryName)

      recovery.recover(subscriber, event)(exception).unsafeRunSync() shouldBe ()

      findProjectCategorySyncTimes(event.projectId) shouldBe Nil
    }

    s"do nothing if there's no row in the subscription_category_sync_times table for the $categoryName and project" in new TestCase {

      findProjectCategorySyncTimes(event.projectId) shouldBe Nil

      recovery.recover(subscriber, event)(exception).unsafeRunSync() shouldBe ()

      findProjectCategorySyncTimes(event.projectId) shouldBe Nil
    }
  }

  private trait TestCase {
    val event = ProjectSyncEvent(projectIds.generateOne, projectPaths.generateOne)

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val recovery         = new DispatchRecoveryImpl[IO](queriesExecTimes)
  }
}
