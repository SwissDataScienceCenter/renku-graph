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

package ch.datascience.tokenrepository.repository.deletion

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.projects.Id
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import eu.timepit.refined.auto._

class TokenRemover[Interpretation[_]](
    transactor:       SessionResource[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[Interpretation, Throwable])
    extends DbClient(Some(queriesExecTimes)) {

  def delete(projectId: Id): Interpretation[Unit] = measureExecutionTime {
    SqlQuery(
      sql"""
          delete 
          from projects_tokens 
          where project_id = ${projectId.value}
          """.update.run
        .map(failIfMultiUpdate(projectId)),
      name = "remove token"
    )
  }.transact(transactor.resource)

  private def failIfMultiUpdate(projectId: Id): Int => Unit = {
    case 0 => ()
    case 1 => ()
    case n => throw new RuntimeException(s"Deleting token for a projectId: $projectId removed $n records")
  }
}
