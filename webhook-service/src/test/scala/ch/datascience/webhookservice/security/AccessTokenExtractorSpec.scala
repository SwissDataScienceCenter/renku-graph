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

package ch.datascience.webhookservice.security

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.http.client.RestClientError.UnauthorizedException
import org.http4s.AuthScheme._
import org.http4s.Credentials.Token
import org.http4s.headers.Authorization
import org.http4s.{Header, Headers, Request}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class AccessTokenExtractorSpec extends WordSpec {

  "findAccessToken" should {

    "return personal access token when PRIVATE-TOKEN is present" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      finder.findAccessToken(
        request.withHeaders(Headers(Header("PRIVATE-TOKEN", accessToken.value)))
      ) shouldBe context.pure(accessToken)
    }

    "return oauth access token when 'AUTHORIZATION: BEARER <token>' is present in the header" in new TestCase {

      val accessToken = oauthAccessTokens.generateOne

      finder.findAccessToken(
        request.withHeaders(Authorization(Token(Bearer, accessToken.value)))
      ) shouldBe context.pure(accessToken)
    }

    "return oauth access token when both PRIVATE-TOKEN and AUTHORIZATION: BEARER are present" in new TestCase {

      val oauthAccessToken    = oauthAccessTokens.generateOne
      val personalAccessToken = personalAccessTokens.generateOne

      finder.findAccessToken(
        request
          .withHeaders(
            Headers(
              Header("PRIVATE-TOKEN", personalAccessToken.value),
              Authorization(Token(Bearer, oauthAccessToken.value))
            )
          )
      ) shouldBe context.pure(oauthAccessToken)
    }

    "fail with UNAUTHORIZED when neither PRIVATE-TOKEN nor AUTHORIZATION: BEARER is not present" in new TestCase {
      finder.findAccessToken(request) shouldBe context.raiseError(UnauthorizedException)
    }

    "fail with UNAUTHORIZED when PRIVATE-TOKEN is invalid" in new TestCase {
      finder.findAccessToken(
        request.withHeaders(Headers(Header("PRIVATE-TOKEN", "")))
      ) shouldBe context.raiseError(UnauthorizedException)
    }

    "fail with UNAUTHORIZED when token in the 'Authorization' header is invalid" in new TestCase {

      val accessToken = oauthAccessTokens.generateOne

      finder.findAccessToken(
        request.withHeaders(Authorization(Token(Basic, accessToken.value)))
      ) shouldBe context.raiseError(UnauthorizedException)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val request = Request[Try]()

    val finder = new AccessTokenExtractor[Try]()
  }
}
