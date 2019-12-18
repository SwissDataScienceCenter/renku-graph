/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect.Bracket
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.tokenrepository.repository.ProjectsTokensDB

import scala.language.higherKinds

class TokenRemover[Interpretation[_]](
    transactor: DbTransactor[Interpretation, ProjectsTokensDB]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  import doobie.implicits._

  def delete(projectId: ProjectId): Interpretation[Unit] =
    sql"""
          delete 
          from projects_tokens 
          where project_id = ${projectId.value}
          """.update.run
      .map(failIfMultiUpdate(projectId))
      .transact(transactor.get)

  private def failIfMultiUpdate(projectId: ProjectId): Int => Unit = {
    case 0 => ()
    case 1 => ()
    case n => throw new RuntimeException(s"Deleting token for a projectId: $projectId removed $n records")
  }
}
