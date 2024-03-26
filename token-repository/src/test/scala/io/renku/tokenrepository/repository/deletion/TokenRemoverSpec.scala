/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository
package deletion

import RepositoryGenerators.deletionResults
import cats.effect._
import cats.syntax.all._
import io.renku.http.client.GitLabGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class TokenRemoverSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "remove the token from DB and revoke project tokens from GL if user access token is given" in {

    val projectId      = projectIds.generateOne
    val deletionResult = deletionResults.generateOne
    givenDBRemoval(projectId, returning = deletionResult.pure[IO])

    val accessToken = accessTokens.generateOne
    givenSuccessfulTokensRevoking(projectId, accessToken)

    tokenRemover.delete(projectId, accessToken.some).asserting(_ shouldBe deletionResult)
  }

  it should "just remove the token from DB if no user access token is given" in {

    val projectId      = projectIds.generateOne
    val deletionResult = deletionResults.generateOne
    givenDBRemoval(projectId, returning = deletionResult.pure[IO])

    tokenRemover.delete(projectId, maybeAccessToken = None).asserting(_ shouldBe deletionResult)
  }

  private val dbTokenRemover    = mock[PersistedTokenRemover[IO]]
  private val tokensRevoker     = mock[TokensRevoker[IO]]
  private lazy val tokenRemover = new TokenRemoverImpl[IO](dbTokenRemover, tokensRevoker)

  private def givenDBRemoval(projectId: projects.GitLabId, returning: IO[DeletionResult]) =
    (dbTokenRemover.delete _)
      .expects(projectId)
      .returning(returning)

  private def givenSuccessfulTokensRevoking(projectId: projects.GitLabId, accessToken: AccessToken) =
    (tokensRevoker.revokeAllTokens _)
      .expects(projectId, Option.empty[AccessTokenId], accessToken)
      .returning(().pure[IO])
}
