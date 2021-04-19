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

package ch.datascience.tokenrepository.repository.init

import cats.effect.Bracket
import cats.syntax.all._
import ch.datascience.db.SessionResource
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import org.typelevel.log4cats.Logger
import skunk.Command
import skunk.data.Completion
import skunk.implicits._

private trait DuplicateProjectsRemover[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object DuplicateProjectsRemover {
  def apply[Interpretation[_]: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, ProjectsTokensDB],
      logger:     Logger[Interpretation]
  ): DuplicateProjectsRemover[Interpretation] =
    new DuplicateProjectsRemoverImpl(transactor, logger)
}

private class DuplicateProjectsRemoverImpl[Interpretation[_]: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, ProjectsTokensDB],
    logger:     Logger[Interpretation]
) extends DuplicateProjectsRemover[Interpretation] {

  override def run(): Interpretation[Unit] = for {
    _ <- deduplicateProjects()
    _ <- logger info "Projects de-duplicated"
  } yield ()

  private def deduplicateProjects() = {

    val query: Command[skunk.Void] = sql"""DELETE FROM projects_tokens WHERE project_id IN (
                                       SELECT project_id
                                       FROM projects_tokens pt
                                       JOIN (
                                         SELECT project_path, MAX(distinct project_id) AS id_to_stay
                                         FROM projects_tokens
                                         GROUP BY project_path
                                         HAVING COUNT(distinct project_id) > 1
                                       ) pr_to_stay ON pr_to_stay.project_path = pt.project_path AND pr_to_stay.id_to_stay <> pt.project_id
                                     )""".command

    transactor.use { session =>
      session.transaction.use { xa =>
        for {
          sp <- xa.savepoint
          _ <- session.execute(query).recoverWith { case e =>
                 xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Completion])
               }

        } yield ()
      }
    }
  }
}
