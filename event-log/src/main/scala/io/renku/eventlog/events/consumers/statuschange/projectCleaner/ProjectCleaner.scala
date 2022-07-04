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

import cats.Applicative
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers.{projectIdEncoder, projectPathEncoder}
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.data.Completion
import skunk.implicits._
import skunk.{Session, SqlState, ~}

import scala.util.control.NonFatal

private[statuschange] trait ProjectCleaner[F[_]] {
  def cleanUp(project: Project): Kleisli[F, Session[F], Unit]
}

private[statuschange] object ProjectCleaner {
  def apply[F[_]: Async: AccessTokenFinder: Logger](queriesExecTimes: LabeledHistogram[F]): F[ProjectCleaner[F]] = for {
    projectWebhookAndTokenRemover <- ProjectWebhookAndTokenRemover[F]
  } yield new ProjectCleanerImpl[F](projectWebhookAndTokenRemover, queriesExecTimes)
}

private[statuschange] class ProjectCleanerImpl[F[_]: Async: Logger](
    projectWebhookAndTokenRemover: ProjectWebhookAndTokenRemover[F],
    queriesExecTimes:              LabeledHistogram[F]
) extends DbClient(Some(queriesExecTimes))
    with ProjectCleaner[F] {
  private val applicative = Applicative[F]
  import applicative._
  import projectWebhookAndTokenRemover._

  override def cleanUp(project: Project): Kleisli[F, Session[F], Unit] = {
    for {
      _       <- removeCleanUpEvents(project)
      _       <- removeProjectSubscriptionSyncTimes(project)
      removed <- removeProject(project)
      _       <- Kleisli.liftF(whenA(removed)(removeWebhookAndToken(project) recoverWith logError(project)))
      _       <- Kleisli.liftF(whenA(removed)(Logger[F].info(show"$categoryName: $project removed")))
    } yield ()
  } recoverWith logWarnAndRetry(project)

  private def logError(project: Project): PartialFunction[Throwable, F[Unit]] = { case NonFatal(error) =>
    Logger[F].error(error)(s"Failed to remove webhook or token for project: ${project.show}")
  }

  private def removeCleanUpEvents(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - clean_up_events_queue removal")
      .command[projects.Id ~ projects.Path](sql"""
        DELETE FROM clean_up_events_queue 
        WHERE project_id = $projectIdEncoder AND project_path = $projectPathEncoder""".command)
      .arguments(project.id ~ project.path)
      .build
      .void
  }

  private def removeProjectSubscriptionSyncTimes(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - subscription_time removal")
      .command[projects.Id ~ projects.Path](sql"""
        DELETE FROM subscription_category_sync_time 
        WHERE project_id IN (
          SELECT st.project_id
          FROM subscription_category_sync_time st
          JOIN project p ON st.project_id = p.project_id 
            AND p.project_id = $projectIdEncoder
            AND p.project_path = $projectPathEncoder
        )""".command)
      .arguments(project.id ~ project.path)
      .build
      .void
  }

  private def removeProject(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - remove project")
      .command[projects.Id ~ projects.Path](sql"""
        DELETE FROM project 
        WHERE project_id = $projectIdEncoder AND project_path = $projectPathEncoder""".command)
      .arguments(project.id ~ project.path)
      .build
      .mapResult {
        case Completion.Delete(1) => true
        case _                    => false
      }
  }

  private def logWarnAndRetry(project: Project): PartialFunction[Throwable, Kleisli[F, Session[F], Unit]] = {
    case SqlState.ForeignKeyViolation(ex) =>
      Kleisli.liftF[F, Session[F], Unit](
        Logger[F].warn(ex)(show"$categoryName: $project removal failed - retrying")
      ) >> cleanUp(project)
  }
}
