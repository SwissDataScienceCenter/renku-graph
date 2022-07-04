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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram

private trait ProjectIdFinder[F[_]] {
  def findProjectId(projectPath: projects.Path): F[Option[projects.Id]]
}

private object ProjectIdFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[ProjectIdFinder[F]] =
    new ProjectIdFinderImpl[F](queriesExecTimes).pure[F].widen
}

private class ProjectIdFinderImpl[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F])
    extends DbClient(Some(queriesExecTimes))
    with ProjectIdFinder[F]
    with TypeSerializers {
  import skunk.implicits._

  override def findProjectId(projectPath: projects.Path): F[Option[projects.Id]] = SessionResource[F].useK {
    measureExecutionTime {
      SqlStatement
        .named(s"${categoryName.show.toLowerCase} - find project_id")
        .select[projects.Path, projects.Id](sql"""
          SELECT project_id
          FROM project
          WHERE project_path = $projectPathEncoder 
        """.query(projectIdDecoder))
        .arguments(projectPath)
        .build(_.option)
    }
  }
}
