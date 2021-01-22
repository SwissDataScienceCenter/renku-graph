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

package ch.datascience.http.server.security

import cats.data.OptionT
import ch.datascience.generators.CommonGraphGenerators.oauthAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.server.security.model.AuthUser
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.Request
import org.http4s.headers.Authorization
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class AuthenticationSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "authUser" should {

    "return a function which succeeds authenticating given request " +
      "if it contains a valid Bearer token" in new TestCase {

        val accessToken = oauthAccessTokens.generateOne

        val authUser   = AuthUser(userGitLabIds.generateOne)
        val authHeader = Authorization(Token(Bearer, accessToken.value))
        (authenticator.authenticate _)
          .expects(authHeader)
          .returning(OptionT.some[Try](authUser))

        authentication.authUser(
          request.withHeaders(authHeader)
        ) shouldBe OptionT.some[Try](authUser)
      }
  }

  private trait TestCase {
    val request = Request[Try]()

    val authenticator  = mock[GitLabAuthenticator[Try]]
    val authentication = new Authentication[Try](authenticator)
  }
}
