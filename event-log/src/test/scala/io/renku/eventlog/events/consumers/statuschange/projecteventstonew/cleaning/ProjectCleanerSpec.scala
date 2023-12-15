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

package io.renku.eventlog.events.consumers.statuschange.projecteventstonew.cleaning

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.consumers.statuschange.categoryName
import io.renku.eventlog.events.producers.SubscriptionProvisioning
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{CleanUpEventsProvisioning, EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewingDeletion
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ProjectCleanerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with CleanUpEventsProvisioning
    with should.Matchers {

  private val project = consumerProjects.generateOne

  "cleanUp" should {

    "remove related records from the subscription_category_sync_time table, " +
      "related records from the the clean_up_events_queue " +
      "and remove the project itself" in testDBResource.use { implicit cfg =>
        for {
          _ <- prepareDB(project)

          otherProject = consumerProjects.generateOne
          _ <- insertCleanUpEvent(otherProject)

          _ = givenProjectViewingDeletionEventSent(project.slug, returning = ().pure[IO])
          _ = givenHookAndTokenRemoval(project, returning = ().pure[IO])

          _ <- logger.resetF()

          _ <- moduleSessionResource(cfg).session.useKleisli(projectCleaner cleanUp project).assertNoException

          _ <- findProjects.asserting(_.find(_.project.id == project.id) shouldBe None)
          _ <- findCategorySyncTimes(project.id).asserting(_ shouldBe Nil)
          _ <- findCleanUpEvents.asserting(_ shouldBe List(Project(otherProject.id, otherProject.slug)))

          _ <- logger.loggedOnlyF(Info(show"$categoryName: $project removed"))
        } yield Succeeded
      }

    "log an error if sending ProjectViewingDeletion event fails" in testDBResource.use { implicit cfg =>
      for {
        _ <- prepareDB(project)

        exception = exceptions.generateOne
        _         = givenProjectViewingDeletionEventSent(project.slug, returning = exception.raiseError[IO, Unit])
        _         = givenHookAndTokenRemoval(project, returning = ().pure[IO])

        _ <- logger.resetF()

        _ <- moduleSessionResource(cfg).session.useKleisli(projectCleaner cleanUp project).assertNoException

        _ <- findProjects.asserting(_.find(_.project.id == project.id) shouldBe None)
        _ <- findCategorySyncTimes(project.id).asserting(_ shouldBe Nil)

        _ <- logger.loggedOnlyF(
               Error(show"$categoryName: sending ProjectViewingDeletion for project: $project failed", exception),
               Info(show"$categoryName: $project removed")
             )
      } yield Succeeded
    }

    "log an error if removal of webhook and token fails" in testDBResource.use { implicit cfg =>
      for {
        _ <- prepareDB(project)

        _         = givenProjectViewingDeletionEventSent(project.slug, returning = ().pure[IO])
        exception = exceptions.generateOne
        _         = givenHookAndTokenRemoval(project, returning = exception.raiseError[IO, Unit])

        _ <- logger.resetF()

        _ <- moduleSessionResource(cfg).session.useKleisli(projectCleaner cleanUp project).assertNoException

        _ <- findProjects.asserting(_.find(_.project.id == project.id) shouldBe None)
        _ <- findCategorySyncTimes(project.id).asserting(_ shouldBe Nil)

        _ <- logger.loggedOnlyF(
               Error(show"$categoryName: removing webhook or token for project: $project failed", exception),
               Info(show"$categoryName: $project removed")
             )
      } yield Succeeded
    }
  }

  private implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private val projectHookRemover  = mock[ProjectWebhookAndTokenRemover[IO]]
  private val tgClient            = mock[triplesgenerator.api.events.Client[IO]]
  private lazy val projectCleaner = new ProjectCleanerImpl[IO](tgClient, projectHookRemover)

  private def prepareDB(project: Project)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    upsertProject(project) >>
      insertCleanUpEvent(project) >>
      upsertCategorySyncTime(project.id, categoryNames.generateOne, lastSyncedDates.generateOne)

  private def givenProjectViewingDeletionEventSent(slug: projects.Slug, returning: IO[Unit]) =
    (tgClient
      .send(_: ProjectViewingDeletion))
      .expects(ProjectViewingDeletion(slug))
      .returning(returning)

  private def givenHookAndTokenRemoval(project: consumers.Project, returning: IO[Unit]) =
    (projectHookRemover.removeWebhookAndToken _).expects(project).returning(returning)
}
