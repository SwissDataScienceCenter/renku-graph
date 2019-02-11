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

import cats.data.OptionT
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.graph.events.ProjectId

import scala.language.higherKinds

private class TokenInRepoFinder[Interpretation[_]: Monad](
    transactorProvider: TransactorProvider[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import doobie.implicits._
  import transactorProvider.transactor

  def findToken(projectId: ProjectId): OptionT[Interpretation, (String, TokenType)] = OptionT {
    sql"select token, token_type from projects_tokens where project_id = ${projectId.value}"
      .query[(String, String)]
      .option
      .transact(transactor)
      .flatMap(toTokenAndTypeTuple(projectId))
  }

  private def toTokenAndTypeTuple(
      projectId: ProjectId
  ): Option[(String, String)] => Interpretation[Option[(String, TokenType)]] = {
    case None                                             => ME.pure(None)
    case Some((encryptedToken, TokenType.Personal.value)) => ME.pure(Some(encryptedToken -> TokenType.Personal))
    case Some((encryptedToken, TokenType.OAuth.value))    => ME.pure(Some(encryptedToken -> TokenType.OAuth))
    case Some((_, tokenType)) =>
      ME.raiseError(new RuntimeException(s"Unknown token type: $tokenType for projectId: $projectId"))
  }
}
