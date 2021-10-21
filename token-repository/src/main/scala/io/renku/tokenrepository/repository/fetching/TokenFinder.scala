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

package io.renku.tokenrepository.repository.fetching

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.AccessToken
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}

private trait TokenFinder[F[_]] {
  def findToken(projectPath: Path): OptionT[F, AccessToken]
  def findToken(projectId:   Id):   OptionT[F, AccessToken]
}

private class TokenFinderImpl[F[_]: MonadThrow](
    tokenInRepoFinder: PersistedTokensFinder[F],
    accessTokenCrypto: AccessTokenCrypto[F]
) extends TokenFinder[F] {

  import accessTokenCrypto._

  override def findToken(projectPath: Path): OptionT[F, AccessToken] = for {
    encryptedToken <- tokenInRepoFinder.findToken(projectPath)
    accessToken    <- OptionT.liftF(decrypt(encryptedToken))
  } yield accessToken

  override def findToken(projectId: Id): OptionT[F, AccessToken] = for {
    encryptedToken <- tokenInRepoFinder.findToken(projectId)
    accessToken    <- OptionT.liftF(decrypt(encryptedToken))
  } yield accessToken
}

private object TokenFinder {
  def apply[F[_]: MonadCancelThrow](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[TokenFinder[F]] = for {
    accessTokenCrypto <- AccessTokenCrypto[F]()
  } yield new TokenFinderImpl[F](
    new PersistedTokensFinderImpl[F](sessionResource, queriesExecTimes),
    accessTokenCrypto
  )
}
