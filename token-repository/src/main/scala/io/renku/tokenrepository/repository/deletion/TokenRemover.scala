/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.deletion

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.graph.model.projects.Id
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryTypeSerializers}
import skunk.data.Completion
import skunk.implicits._

private[repository] trait TokenRemover[F[_]] {
  def delete(projectId: Id): F[Unit]
}

private[repository] class TokenRemoverImpl[F[_]: MonadCancelThrow](
    sessionResource:  SessionResource[F, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient[F](Some(queriesExecTimes))
    with TokenRemover[F]
    with TokenRepositoryTypeSerializers {

  override def delete(projectId: Id): F[Unit] = sessionResource.useK {
    measureExecutionTime {
      SqlStatement(name = "remove token")
        .command[Id](sql"""delete from projects_tokens
                           where project_id = $projectIdEncoder""".command)
        .arguments(projectId)
        .build
        .flatMapResult(failIfMultiUpdate(projectId))
    }
  }

  private def failIfMultiUpdate(projectId: Id): Completion => F[Unit] = {
    case Completion.Delete(0 | 1) => ().pure[F]
    case Completion.Delete(n) =>
      new RuntimeException(s"Deleting token for a projectId: $projectId removed $n records")
        .raiseError[F, Unit]
    case completion =>
      new RuntimeException(s"Deleting token for a projectId: $projectId failed with completion code $completion")
        .raiseError[F, Unit]
  }
}
