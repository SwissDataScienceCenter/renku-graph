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

package ch.datascience.tokenrepository.repository

import cats.{Monad, MonadError}
import ch.datascience.db.TransactorProvider
import ch.datascience.graph.events.ProjectId

import scala.language.higherKinds

private class TokenAssociator[Interpretation[_]: Monad](
    transactorProvider: TransactorProvider[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import doobie.implicits._
  import transactorProvider.transactor

  def associate(projectId: ProjectId, encryptedToken: String, tokenType: TokenType): Interpretation[Unit] =
    sql"select token from projects_tokens where project_id = ${projectId.value}"
      .query[String]
      .option
      .flatMap {
        case Some(_) => update(projectId, encryptedToken, tokenType)
        case None    => insert(projectId, encryptedToken, tokenType)
      }
      .transact(transactor)

  private def insert(projectId: ProjectId, encryptedToken: String, tokenType: TokenType) =
    sql"""insert into 
          projects_tokens (project_id, token, token_type) 
          values (${projectId.value}, $encryptedToken, ${tokenType.value})
      """.update.run.map(failIfMultiUpdate(projectId))

  private def update(projectId: ProjectId, encryptedToken: String, tokenType: TokenType) =
    sql"""update projects_tokens 
                set token = $encryptedToken, token_type = ${tokenType.value}
                where project_id = ${projectId.value}
            """.update.run.map(failIfMultiUpdate(projectId))

  private def failIfMultiUpdate(projectId: ProjectId): Int => Unit = {
    case 1 => ()
    case _ => new RuntimeException(s"Associating token for a projectId: $projectId")
  }
}
