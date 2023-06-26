/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.deletion

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.tokenrepository.repository.AccessTokenId
import io.renku.tokenrepository.repository.RepositoryGenerators.accessTokenIds
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import scala.util.Random

class TokensRevokerSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "revoke all found project tokens except from the given one" in {

    val projectId       = projectIds.generateOne
    val userAccessToken = accessTokens.generateOne
    val tokenToLeave    = accessTokenIds.generateOne
    val tokensToRemove  = accessTokenIds.generateFixedSizeList(1)

    givenStreamOfTokensToRevoke(
      projectId,
      userAccessToken,
      returning = Stream.emits[IO, AccessTokenId](Random.shuffle(tokenToLeave :: tokensToRemove))
    )

    tokensToRemove foreach (givenTokenRevoking(projectId, _: AccessTokenId, userAccessToken, returning = ().pure[IO]))

    tokensRevoker.revokeAllTokens(projectId, tokenToLeave.some, userAccessToken).assertNoException >>
      logger.expectNoLogs().pure[IO]
  }

  it should "revoke all found project tokens when no token to leave is given" in {

    val projectId       = projectIds.generateOne
    val userAccessToken = accessTokens.generateOne
    val tokensToRemove  = accessTokenIds.generateList()

    givenStreamOfTokensToRevoke(
      projectId,
      userAccessToken,
      returning = Stream.fromIterator[IO](tokensToRemove.iterator, chunkSize = 20)
    )

    tokensToRemove foreach (givenTokenRevoking(projectId, _: AccessTokenId, userAccessToken, returning = ().pure[IO]))

    tokensRevoker.revokeAllTokens(projectId, except = None, userAccessToken).assertNoException >>
      logger.expectNoLogs().pure[IO]
  }

  it should "log a warning and succeed when token revoking fails" in {

    val projectId       = projectIds.generateOne
    val userAccessToken = accessTokens.generateOne

    val exception = exceptions.generateOne
    givenStreamOfTokensToRevoke(projectId, userAccessToken, returning = Stream.raiseError[IO](exception))

    tokensRevoker.revokeAllTokens(projectId, except = None, userAccessToken).assertNoException >>
      logger.logged(Warn(show"removing old token in GitLab for project $projectId failed", exception)).pure[IO]
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val revokeCandidatesFinder = mock[RevokeCandidatesFinder[IO]]
  private lazy val tokenRevoker           = mock[TokenRevoker[IO]]
  private lazy val tokensRevoker          = new TokensRevokerImpl[IO](revokeCandidatesFinder, tokenRevoker)

  private def givenTokenRevoking(projectId:   projects.GitLabId,
                                 tokenId:     AccessTokenId,
                                 accessToken: AccessToken,
                                 returning:   IO[Unit]
  ) = (tokenRevoker.revokeToken _)
    .expects(tokenId, projectId, accessToken)
    .returning(returning)

  private def givenStreamOfTokensToRevoke(projectId:   projects.GitLabId,
                                          accessToken: AccessToken,
                                          returning:   Stream[IO, AccessTokenId]
  ) = (revokeCandidatesFinder.projectAccessTokensStream _)
    .expects(projectId, accessToken)
    .returning(returning)
}
