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

package io.renku.eventlog.events.categories.projectsync

import cats.MonadThrow
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram

private trait DBUpdater[F[_]] {
  def update(projectId: projects.Id, newPath: projects.Path): F[Unit]
}

private class DBUpdaterImpl[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F])
    extends DbClient(Some(queriesExecTimes))
    with DBUpdater[F]
    with TypeSerializers {
  import skunk._
  import skunk.data.Completion
  import skunk.implicits._

  override def update(projectId: projects.Id, newPath: projects.Path): F[Unit] = SessionResource[F].useK {
    updateProject(projectId, newPath)
  }

  private def updateProject(projectId: projects.Id, newPath: projects.Path) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - update project")
      .command[projects.Path ~ projects.Id](sql"""
        UPDATE project
        SET project_path = $projectPathEncoder
        WHERE project_id = $projectIdEncoder
      """.command)
      .arguments(newPath, projectId)
      .build
      .flatMapResult {
        case Completion.Update(0 | 1) => ().pure[F]
        case completion => new Exception(s"Failed updating project $projectId: $completion").raiseError[F, Unit]
      }
  }
}

private object DBUpdater {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[DBUpdater[F]] =
    MonadThrow[F].catchNonFatal(new DBUpdaterImpl[F](queriesExecTimes: LabeledHistogram[F])).widen
}
