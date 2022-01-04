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

package io.renku.eventlog.events.categories.statuschange.projectCleaner

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers.projectIdEncoder
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.Session
import skunk.implicits.toStringOps

import scala.util.control.NonFatal

private[statuschange] trait ProjectCleaner[F[_]] {
  def cleanUp(project: Project): Kleisli[F, Session[F], Unit]
}

private[statuschange] object ProjectCleaner {
  def apply[F[_]: Async: Logger](queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]): F[ProjectCleaner[F]] = for {
    projectWebhookAndTokenRemover <- ProjectWebhookAndTokenRemover[F]()
  } yield new ProjectCleanerImpl[F](projectWebhookAndTokenRemover, queriesExecTimes)
}

private[statuschange] class ProjectCleanerImpl[F[_]: Async: Logger](
    projectWebhookAndTokenRemover: ProjectWebhookAndTokenRemover[F],
    queriesExecTimes:              LabeledHistogram[F, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with ProjectCleaner[F] {

  import projectWebhookAndTokenRemover._

  override def cleanUp(project: Project): Kleisli[F, Session[F], Unit] = for {
    _ <- removeProjectSubscriptionSyncTimes(project)
    _ <- removeProject(project)
    _ <- Kleisli.liftF[F, Session[F], Unit](removeWebhookAndToken(project)) recoverWith logError(project)
  } yield ()

  private def logError(project: Project): PartialFunction[Throwable, Kleisli[F, Session[F], Unit]] = {
    case NonFatal(error) =>
      Kleisli.liftF(Logger[F].error(error)(s"Failed to remove webhook or token for project: ${project.show}"))
  }

  private def removeProjectSubscriptionSyncTimes(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - subscription_time removal")
      .command[projects.Id](
        sql"""DELETE FROM subscription_category_sync_time WHERE project_id = $projectIdEncoder""".command
      )
      .arguments(project.id)
      .build
      .void
  }

  private def removeProject(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - remove project")
      .command[projects.Id](
        sql"""DELETE FROM project WHERE project_id = $projectIdEncoder""".command
      )
      .arguments(project.id)
      .build
      .void
  }

}
