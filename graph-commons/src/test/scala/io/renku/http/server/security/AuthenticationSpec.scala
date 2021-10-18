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

package io.renku.http.server.security

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.http.ErrorMessage._
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.server.security.model.AuthUser
import org.http4s.dsl.Http4sDsl
import org.http4s.{AuthedRoutes, Request, Response}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class AuthenticationSpec extends AnyWordSpec with should.Matchers with MockFactory with ScalaCheckPropertyChecks {

  "authenticateInNeeded" should {

    val scenarios = Table(
      "Token type"            -> "Value",
      "Personal Access Token" -> personalAccessTokens.generateOne,
      "OAuth Access Token"    -> oauthAccessTokens.generateOne
    )

    forAll(scenarios) { (tokenType, accessToken) =>
      "return a function which succeeds authenticating " +
        s"if the given $tokenType is valid" in new TestCase {

          val authUser = authUsers.generateOne.copy(accessToken = accessToken)
          (authenticator.authenticate _)
            .expects(accessToken)
            .returning(authUser.asRight[EndpointSecurityException].pure[Try])

          authentication.authenticateIfNeeded(
            request.withHeaders(accessToken.toHeader)
          ) shouldBe authUser.some.asRight[EndpointSecurityException].pure[Try]
        }
    }

    "return a function which succeeds authenticating given request and return no user " +
      "if the request does not contain an Authorization token" in new TestCase {
        authentication
          .authenticateIfNeeded(request) shouldBe Option.empty[AuthUser].asRight[EndpointSecurityException].pure[Try]
      }

    "return a function which fails authenticating the given request " +
      "if it contains an Authorization token that gets rejected by the authenticator" in new TestCase {

        val accessToken = accessTokens.generateOne
        val exception   = securityExceptions.generateOne
        (authenticator.authenticate _)
          .expects(accessToken)
          .returning(exception.asLeft[AuthUser].pure[Try])

        authentication
          .authenticateIfNeeded(
            request.withHeaders(accessToken.toHeader)
          ) shouldBe exception.asLeft[Option[AuthUser]].pure[Try]
      }
  }

  "authenticate" should {

    val scenarios = Table(
      "Token type"            -> "Value",
      "Personal Access Token" -> personalAccessTokens.generateOne,
      "OAuth Access Token"    -> oauthAccessTokens.generateOne
    )

    forAll(scenarios) { (tokenType, accessToken) =>
      "return a function which succeeds authenticating " +
        s"if the given $tokenType is valid" in new TestCase {

          val authUser = authUsers.generateOne.copy(accessToken = accessToken)
          (authenticator.authenticate _)
            .expects(accessToken)
            .returning(authUser.asRight[EndpointSecurityException].pure[Try])

          authentication.authenticate(
            request.withHeaders(accessToken.toHeader)
          ) shouldBe authUser.asRight[EndpointSecurityException].pure[Try]
        }
    }

    "return a function which fails authenticating the given request " +
      "if the it does not contain an Authorization token" in new TestCase {
        authentication.authenticate(request) shouldBe AuthenticationFailure.asLeft[AuthUser].pure[Try]
      }

    "return a function which fails authenticating the given request " +
      "if it contains an Authorization token that gets rejected by the authenticator" in new TestCase {

        val accessToken = accessTokens.generateOne
        val exception   = securityExceptions.generateOne
        (authenticator.authenticate _)
          .expects(accessToken)
          .returning(exception.asLeft[AuthUser].pure[Try])

        authentication
          .authenticate(request.withHeaders(accessToken.toHeader)) shouldBe exception.asLeft[AuthUser].pure[Try]
      }
  }

  "middlewareAuthenticatingIfNeeded" should {

    "return Unauthorized for unauthorized requests" in new Http4sDsl[IO] {
      val authentication = mock[Authentication[IO]]

      val exception = securityExceptions.generateOne
      val authenticate: Kleisli[IO, Request[IO], Either[EndpointSecurityException, Option[AuthUser]]] =
        Kleisli.liftF(exception.asLeft[Option[AuthUser]].pure[IO])

      (() => authentication.authenticateIfNeeded)
        .expects()
        .returning(authenticate)

      val request = Request[IO]()

      val maybeResponse = Authentication.middlewareAuthenticatingIfNeeded(authentication) {
        AuthedRoutes.of { case GET -> Root as _ => Response.notFound[IO].pure[IO] }
      }(request)

      val Some(response) = maybeResponse.value.unsafeRunSync()

      val expectedResponse = exception.toHttpResponse[IO]
      response.status                           shouldBe expectedResponse.status
      response.contentType                      shouldBe expectedResponse.contentType
      response.as[ErrorMessage].unsafeRunSync() shouldBe expectedResponse.as[ErrorMessage].unsafeRunSync()
    }
  }

  "middleware" should {

    "return Unauthorized for unauthorized requests" in new Http4sDsl[IO] {
      val authentication = mock[Authentication[IO]]

      val exception = securityExceptions.generateOne
      val authenticate: Kleisli[IO, Request[IO], Either[EndpointSecurityException, AuthUser]] =
        Kleisli.liftF(exception.asLeft[AuthUser].pure[IO])

      (() => authentication.authenticate)
        .expects()
        .returning(authenticate)

      val request = Request[IO]()

      val maybeResponse = Authentication.middleware(authentication) {
        AuthedRoutes.of { case GET -> Root as _ => Response.notFound[IO].pure[IO] }
      }(request)

      val Some(response) = maybeResponse.value.unsafeRunSync()

      val expectedResponse = exception.toHttpResponse[IO]
      response.status                           shouldBe expectedResponse.status
      response.contentType                      shouldBe expectedResponse.contentType
      response.as[ErrorMessage].unsafeRunSync() shouldBe expectedResponse.as[ErrorMessage].unsafeRunSync()
    }
  }

  private trait TestCase {
    val request = Request[Try]()

    val authenticator  = mock[Authenticator[Try]]
    val authentication = new AuthenticationImpl[Try](authenticator)
  }
}
