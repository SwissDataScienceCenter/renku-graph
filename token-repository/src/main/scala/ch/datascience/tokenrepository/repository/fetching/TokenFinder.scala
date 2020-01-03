/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.client.AccessToken
import ch.datascience.tokenrepository.repository._

import scala.language.higherKinds

private class TokenFinder[Interpretation[_]](
    tokenInRepoFinder: PersistedTokensFinder[Interpretation],
    accessTokenCrypto: AccessTokenCrypto[Interpretation]
)(implicit ME:         MonadError[Interpretation, Throwable]) {

  import accessTokenCrypto._

  def findToken(projectPath: ProjectPath): OptionT[Interpretation, AccessToken] =
    for {
      encryptedToken <- tokenInRepoFinder.findToken(projectPath)
      accessToken    <- OptionT.liftF(decrypt(encryptedToken))
    } yield accessToken

  def findToken(projectId: ProjectId): OptionT[Interpretation, AccessToken] =
    for {
      encryptedToken <- tokenInRepoFinder.findToken(projectId)
      accessToken    <- OptionT.liftF(decrypt(encryptedToken))
    } yield accessToken
}

private object IOTokenFinder {
  def apply(
      transactor:          DbTransactor[IO, ProjectsTokensDB]
  )(implicit contextShift: ContextShift[IO]): IO[TokenFinder[IO]] =
    for {
      accessTokenCrypto <- AccessTokenCrypto[IO]()
    } yield new TokenFinder[IO](
      new IOPersistedTokensFinder(transactor),
      accessTokenCrypto
    )
}
