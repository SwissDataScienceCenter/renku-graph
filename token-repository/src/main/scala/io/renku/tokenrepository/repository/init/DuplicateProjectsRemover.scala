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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.tokenrepository.repository.ProjectsTokensDB
import org.typelevel.log4cats.Logger
import skunk.implicits._
import skunk.{Command, Session}

private trait DuplicateProjectsRemover[F[_]] {
  def run(): F[Unit]
}

private object DuplicateProjectsRemover {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, ProjectsTokensDB]
  ): DuplicateProjectsRemover[F] = new DuplicateProjectsRemoverImpl(sessionResource)
}

private class DuplicateProjectsRemoverImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, ProjectsTokensDB]
) extends DuplicateProjectsRemover[F] {

  override def run(): F[Unit] = sessionResource.useK {
    for {
      _ <- deduplicateProjects()
      _ <- Kleisli.liftF(Logger[F] info "Projects de-duplicated")
    } yield ()
  }

  private def deduplicateProjects(): Kleisli[F, Session[F], Unit] = {

    val query: Command[skunk.Void] =
      sql"""DELETE FROM projects_tokens WHERE project_id IN (
                                       SELECT project_id
                                       FROM projects_tokens pt
                                       JOIN (
                                         SELECT project_path, MAX(distinct project_id) AS id_to_stay
                                         FROM projects_tokens
                                         GROUP BY project_path
                                         HAVING COUNT(distinct project_id) > 1
                                       ) pr_to_stay ON pr_to_stay.project_path = pt.project_path AND pr_to_stay.id_to_stay <> pt.project_id
                                     )""".command
    Kleisli(_.execute(query).void)
  }
}
