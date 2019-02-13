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

package ch.datascience.tokenrepository.repository.fetching

import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.db.TransactorProvider
import ch.datascience.graph.events.ProjectId
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensConfig

import scala.language.higherKinds

private class PersistedTokensFinder[Interpretation[_]: Monad](
    transactorProvider: TransactorProvider[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import doobie.implicits._

  def findToken(projectId: ProjectId): OptionT[Interpretation, EncryptedAccessToken] = OptionT {
    for {
      transactor <- transactorProvider.transactor
      token <- sql"select token from projects_tokens where project_id = ${projectId.value}"
                .query[String]
                .option
                .transact(transactor)
                .flatMap(toSerializedAccessToken(projectId))

    } yield token
  }

  private def toSerializedAccessToken(
      projectId: ProjectId
  ): Option[String] => Interpretation[Option[EncryptedAccessToken]] = {
    case None => ME.pure(None)
    case Some(encryptedToken) =>
      ME.fromEither {
        EncryptedAccessToken.from(encryptedToken).map(Option.apply)
      }
  }
}

private class IOPersistedTokensFinder(implicit contextShift: ContextShift[IO])
    extends PersistedTokensFinder[IO](new TransactorProvider[IO](new ProjectsTokensConfig[IO]))
