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
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.server.security.model.AuthUser
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.http4s.{Header, Request}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class AuthenticationSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "authenticate" should {

    "return a function which succeeds authenticating given request and return an authenticated user " +
      "if the request contains a valid Authorization token" in new TestCase {

        val authUser   = AuthUser(userGitLabIds.generateOne)
        val authHeader = authHeaders.generateOne
        (authenticator.authenticate _)
          .expects(authHeader)
          .returning(OptionT.some[Try](authUser))

        authentication.authenticate(
          request.withHeaders(authHeader)
        ) shouldBe OptionT.some[Try](Some(authUser))
      }

    "return a function which succeeds authenticating given request and return no user " +
      "if the request does not contain a Authorization token" in new TestCase {
        authentication.authenticate(request) shouldBe OptionT.some[Try](None)
      }

    "return a function which fails authentication of the given request " +
      "if it contains an Authorization token that gets rejected by the authenticator" in new TestCase {

        val authHeader = authHeaders.generateOne
        val exception  = exceptions.generateOne
        (authenticator.authenticate _)
          .expects(authHeader)
          .returning(OptionT.liftF(exception.raiseError[Try, AuthUser]))

        authentication
          .authenticate(
            request.withHeaders(authHeader)
          )
          .value shouldBe exception.raiseError[Try, Option[AuthUser]]
      }

    "return a function which returns None if authenticator returns no user " +
      "for the header from the given request" in new TestCase {

        val authHeader = authHeaders.generateOne
        (authenticator.authenticate _)
          .expects(authHeader)
          .returning(OptionT.none[Try, AuthUser])

        authentication.authenticate(request.withHeaders(authHeader)) shouldBe OptionT.none[Try, Option[AuthUser]]
      }
  }

  private trait TestCase {
    val request = Request[Try]()

    val authenticator  = mock[GitLabAuthenticator[Try]]
    val authentication = new Authentication[Try](authenticator)
  }

  private lazy val authHeaders: Gen[Header] =
    accessTokens map {
      case PersonalAccessToken(token) => Header("PRIVATE-TOKEN", token)
      case OAuthAccessToken(token)    => Authorization(Token(Bearer, token))
    }
}
