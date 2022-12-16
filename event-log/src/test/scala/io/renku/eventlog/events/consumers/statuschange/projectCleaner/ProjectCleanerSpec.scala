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

package io.renku.eventlog.events.consumers.statuschange
package projectCleaner

import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog.events.producers.SubscriptionDataProvisioning
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{CleanUpEventsProvisioning, InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.LastSyncedDate
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectCleanerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with CleanUpEventsProvisioning
    with SubscriptionDataProvisioning
    with TypeSerializers
    with MockFactory
    with should.Matchers {

  "cleanUp" should {

    "remove related records from the subscription_category_sync_time table, " +
      "related records from the the clean_up_events_queue " +
      "and remove the project itself" in new TestCase {

        val otherProject = consumerProjects.generateOne
        insertCleanUpEvent(otherProject)

        (projectHookRemover.removeWebhookAndToken _).expects(project).returns(().pure[IO])

        sessionResource.useK(projectCleaner cleanUp project).unsafeRunSync()

        findProjects.find(_._1 == project.id)    shouldBe None
        findProjectCategorySyncTimes(project.id) shouldBe List.empty[(CategoryName, LastSyncedDate)]
        findCleanUpEvents                        shouldBe List(otherProject.id -> otherProject.path)

        logger.loggedOnly(Info(show"$categoryName: $project removed"))
      }

    "log an error if the removal of the webhook fails" in new TestCase {
      val exception = exceptions.generateOne
      (projectHookRemover.removeWebhookAndToken _).expects(project).returns(exception.raiseError[IO, Unit])

      sessionResource.useK(projectCleaner cleanUp project).unsafeRunSync()

      findProjects.find(_._1 == project.id)    shouldBe None
      findProjectCategorySyncTimes(project.id) shouldBe List.empty[(CategoryName, LastSyncedDate)]

      logger.loggedOnly(
        Error(show"Failed to remove webhook or token for project: $project", exception),
        Info(show"$categoryName: $project removed")
      )
    }
  }

  private trait TestCase {
    val project = consumerProjects.generateOne

    upsertProject(project.id, project.path, eventDates.generateOne)
    insertCleanUpEvent(project)
    upsertCategorySyncTime(project.id, categoryNames.generateOne, lastSyncedDates.generateOne)

    implicit val logger:                   TestLogger[IO]            = TestLogger[IO]()
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val projectHookRemover = mock[ProjectWebhookAndTokenRemover[IO]]
    val projectCleaner     = new ProjectCleanerImpl[IO](projectHookRemover)
  }
}
