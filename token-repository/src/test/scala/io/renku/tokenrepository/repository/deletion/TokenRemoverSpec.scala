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

package io.renku.tokenrepository.repository
package deletion

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class TokenRemoverSpec extends AnyFlatSpec with should.Matchers with TryValues with MockFactory {

  it should "remove the token from DB and revoke project tokens from GL if user access token is given" in new TestCase {

    val projectId = projectIds.generateOne
    givenDBRemoval(projectId, returning = ().pure[Try])

    val accessToken = accessTokens.generateOne
    givenSuccessfulTokensRevoking(projectId, accessToken)

    tokenRemover.delete(projectId, accessToken.some).success.value shouldBe ()
  }

  it should "just remove the token from DB if no user access token is given" in new TestCase {

    val projectId = projectIds.generateOne
    givenDBRemoval(projectId, returning = ().pure[Try])

    tokenRemover.delete(projectId, maybeAccessToken = None).success.value shouldBe ()
  }

  private trait TestCase {

    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    private val dbTokenRemover = mock[PersistedTokenRemover[Try]]
    private val tokensRevoker  = mock[TokensRevoker[Try]]
    val tokenRemover           = new TokenRemoverImpl[Try](dbTokenRemover, tokensRevoker)

    def givenDBRemoval(projectId: projects.GitLabId, returning: Try[Unit]) =
      (dbTokenRemover.delete _)
        .expects(projectId)
        .returning(returning)

    def givenSuccessfulTokensRevoking(projectId: projects.GitLabId, accessToken: AccessToken) =
      (tokensRevoker.revokeAllTokens _)
        .expects(projectId, Option.empty[AccessTokenId], accessToken)
        .returning(().pure[Try])
  }
}
