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

package io.renku.eventlog.events.consumers.statuschange
package projecteventstonew.cleaning

import cats.Applicative
import cats.data.Kleisli
import cats.data.Kleisli.liftF
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers.{projectIdEncoder, projectSlugEncoder}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewingDeletion
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

private[statuschange] trait ProjectCleaner[F[_]] {
  def cleanUp(project: Project): Kleisli[F, Session[F], Unit]
}

private[statuschange] object ProjectCleaner {
  def apply[F[_]: Async: Logger: QueriesExecutionTimes: MetricsRegistry]: F[ProjectCleaner[F]] =
    for {
      tgClient                      <- triplesgenerator.api.events.Client[F]
      projectWebhookAndTokenRemover <- ProjectWebhookAndTokenRemover[F]
    } yield new ProjectCleanerImpl[F](tgClient, projectWebhookAndTokenRemover)
}

private[statuschange] class ProjectCleanerImpl[F[_]: Async: Logger: QueriesExecutionTimes](
    tgClient:                      triplesgenerator.api.events.Client[F],
    projectWebhookAndTokenRemover: ProjectWebhookAndTokenRemover[F]
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with ProjectCleaner[F] {
  private val applicative = Applicative[F]
  import applicative._

  override def cleanUp(project: Project): Kleisli[F, Session[F], Unit] =
    for {
      _       <- removeCleanUpEvents(project)
      _       <- removeProjectSubscriptionSyncTimes(project)
      removed <- removeProject(project)
      _       <- liftF(whenA(removed)(sendProjectViewingDeletion(project)))
      _       <- liftF(whenA(removed)(removeWebhookAndToken(project)))
      _       <- liftF(whenA(removed)(Logger[F].info(show"$categoryName: $project removed")))
    } yield ()

  private def sendProjectViewingDeletion(project: Project) =
    tgClient
      .send(ProjectViewingDeletion(project.slug))
      .handleErrorWith(logError(project, "sending ProjectViewingDeletion"))

  private def removeWebhookAndToken(project: Project) =
    projectWebhookAndTokenRemover
      .removeWebhookAndToken(project)
      .handleErrorWith(logError(project, "removing webhook or token"))

  private def logError(project: Project, message: String): Throwable => F[Unit] = { error =>
    Logger[F].error(error)(s"$categoryName: $message for project: ${project.show} failed")
  }

  private def removeCleanUpEvents(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - clean_up_events_queue removal")
      .command[projects.GitLabId *: projects.Slug *: EmptyTuple](sql"""
        DELETE FROM clean_up_events_queue 
        WHERE project_id = $projectIdEncoder AND project_slug = $projectSlugEncoder""".command)
      .arguments(project.id *: project.slug *: EmptyTuple)
      .build
      .void
  }

  private def removeProjectSubscriptionSyncTimes(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - subscription_time removal")
      .command[projects.GitLabId *: projects.Slug *: EmptyTuple](sql"""
        DELETE FROM subscription_category_sync_time 
        WHERE project_id IN (
          SELECT st.project_id
          FROM subscription_category_sync_time st
          JOIN project p ON st.project_id = p.project_id 
            AND p.project_id = $projectIdEncoder
            AND p.project_slug = $projectSlugEncoder
        )""".command)
      .arguments(project.id *: project.slug *: EmptyTuple)
      .build
      .void
  }

  private def removeProject(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - remove project")
      .command[projects.GitLabId *: projects.Slug *: EmptyTuple](sql"""
        DELETE FROM project 
        WHERE project_id = $projectIdEncoder AND project_slug = $projectSlugEncoder""".command)
      .arguments(project.id *: project.slug *: EmptyTuple)
      .build
      .mapResult {
        case Completion.Delete(1) => true
        case _                    => false
      }
  }
}
