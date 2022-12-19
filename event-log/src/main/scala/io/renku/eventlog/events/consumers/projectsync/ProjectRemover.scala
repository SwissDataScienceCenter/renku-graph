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

package io.renku.eventlog.events.consumers.projectsync

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.projects

private trait ProjectRemover[F[_]] {
  def removeProject(projectId: projects.GitLabId): F[Unit]
}

private class ProjectRemoverImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with ProjectRemover[F]
    with TypeSerializers {
  import skunk._
  import skunk.data.Completion
  import skunk.implicits._

  override def removeProject(projectId: projects.GitLabId): F[Unit] = SessionResource[F].useWithTransactionK[Unit] {
    Kleisli { case (tx, session) =>
      for {
        sp <- tx.savepoint
        _  <- deleteAll(projectId)(session).recoverWith { case err => tx.rollback(sp) >> err.raiseError[F, Unit] }
        _  <- tx.commit
      } yield ()
    }
  }

  private def deleteAll(implicit id: projects.GitLabId): Kleisli[F, Session[F], Unit] =
    delete("payloads", sql"""DELETE FROM event_payload WHERE project_id = $projectIdEncoder""".command) >>
      delete("processing times",
             sql"""DELETE FROM status_processing_time WHERE project_id = $projectIdEncoder""".command
      ) >>
      delete("events deliveries", sql"""DELETE FROM event_delivery WHERE project_id = $projectIdEncoder""".command) >>
      delete("events", sql"""DELETE FROM event WHERE project_id = $projectIdEncoder""".command) >>
      delete("sync times",
             sql"""DELETE FROM subscription_category_sync_time WHERE project_id = $projectIdEncoder""".command
      ) >>
      delete("clean-up events",
             sql"""DELETE FROM clean_up_events_queue WHERE project_id = $projectIdEncoder""".command
      ) >>
      delete("project", sql"""DELETE FROM project WHERE project_id = $projectIdEncoder""".command)

  private def delete(matter: String, sql: Command[projects.GitLabId])(implicit projectId: projects.GitLabId) =
    measureExecutionTime {
      SqlStatement
        .named(s"${categoryName.value.toLowerCase} - delete $matter")
        .command[projects.GitLabId](sql)
        .arguments(projectId)
        .build
        .flatMapResult {
          case Completion.Delete(_) => ().pure[F]
          case c => new Exception(show"Deleting $matter with projectId = $projectId failed: $c").raiseError[F, Unit]
        }
    }
}

private object ProjectRemover {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: F[ProjectRemover[F]] =
    MonadThrow[F].catchNonFatal(new ProjectRemoverImpl[F]).widen
}
