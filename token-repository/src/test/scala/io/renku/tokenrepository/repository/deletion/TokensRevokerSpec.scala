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

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import io.renku.tokenrepository.repository.AccessTokenId
import io.renku.tokenrepository.repository.RepositoryGenerators.accessTokenIds
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class TokensRevokerSpec extends AnyFlatSpec with should.Matchers with TryValues with MockFactory {

  it should "succeed and revoke all found project tokens" in new TestCase {

    val projectId     = projectIds.generateOne
    val accessToken   = accessTokens.generateOne
    val projectTokens = accessTokenIds.generateList()

    givenTokensToRevokeFinding(projectId, accessToken, returning = projectTokens.pure[Try])

    projectTokens foreach (givenTokenRevoking(projectId, _: AccessTokenId, accessToken, returning = ().pure[Try]))

    tokensRevoker.revokeAllTokens(projectId, accessToken).success.value shouldBe ()
  }

  it should "log a warning and succeed when token revoking process fails" in new TestCase {

    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    val exception = exceptions.generateOne
    givenTokensToRevokeFinding(projectId, accessToken, returning = exception.raiseError[Try, Nothing])

    tokensRevoker.revokeAllTokens(projectId, accessToken).success.value shouldBe ()

    logger.logged(
      Warn(show"removing old token in GitLab for project $projectId failed", exception)
    )
  }

  private trait TestCase {

    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    private val revokeCandidatesFinder = mock[RevokeCandidatesFinder[Try]]
    private val tokenRevoker           = mock[TokenRevoker[Try]]
    val tokensRevoker                  = new TokensRevokerImpl[Try](revokeCandidatesFinder, tokenRevoker)

    def givenTokenRevoking(projectId:   projects.GitLabId,
                           tokenId:     AccessTokenId,
                           accessToken: AccessToken,
                           returning:   Try[Unit]
    ) = (tokenRevoker.revokeToken _)
      .expects(projectId, tokenId, accessToken)
      .returning(returning)

    def givenTokensToRevokeFinding(projectId:   projects.GitLabId,
                                   accessToken: AccessToken,
                                   returning:   Try[List[AccessTokenId]]
    ) = (revokeCandidatesFinder.findProjectAccessTokens _)
      .expects(projectId, accessToken)
      .returning(returning)
  }
}
