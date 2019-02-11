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

import cats.MonadError
import cats.data.OptionT
import cats.effect.IO
import ch.datascience.clients.AccessToken
import ch.datascience.clients.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.graph.events.ProjectId

import scala.language.higherKinds

private class TokenFinder[Interpretation[_]](
    tokenInRepoFinder: TokenInRepoFinder[Interpretation]
)(implicit ME:         MonadError[Interpretation, Throwable]) {

  def findToken(projectId: ProjectId): OptionT[Interpretation, AccessToken] =
    tokenInRepoFinder.findToken(projectId).flatMap(toAccessToken)

  private lazy val toAccessToken: ((String, TokenType)) => OptionT[Interpretation, AccessToken] = {
    case (token, TokenType.OAuth)    => toOptionT(OAuthAccessToken.from(token))
    case (token, TokenType.Personal) => toOptionT(PersonalAccessToken.from(token))
  }

  private def toOptionT[T](maybeValue: Either[IllegalArgumentException, T]): OptionT[Interpretation, T] =
    OptionT.liftF(ME.fromEither(maybeValue))
}

private object IOTokenFinder extends TokenFinder[IO](new TokenInRepoFinder[IO](IOTransactorProvider))
