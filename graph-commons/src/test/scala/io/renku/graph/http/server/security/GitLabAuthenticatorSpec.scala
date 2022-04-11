/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.http.server.security

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.server.security._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Status._
import org.http4s.circe.jsonEncoder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Header, Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabAuthenticatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with ExternalServiceStubbing
    with GitLabClientTools[IO] {

  "authenticate" should {

    "return the user if the token is valid" in new TestCase {
      val userId = personGitLabIds.generateOne

      val result = Right(AuthUser(userId, accessToken))

      (gitLabClient
        .get(_: Uri, _: NonEmptyString)(_: ResponseMappingF[IO, Either[EndpointSecurityException, AuthUser]])(
          _: Option[AccessToken]
        ))
        .expects(uri, endpointName, *, accessToken.some)
        .returning(result.pure[IO])

      authenticator.authenticate(accessToken).unsafeRunSync() shouldBe result
    }

    // mapResponse

    "use the given access token to call GitLab's GET /user endpoint " +
      "and return authorized user if GitLab responds with OK" in new TestCase {

        val userId = personGitLabIds.generateOne

        mapResponse(Status.Ok, Request(), Response().withEntity(json"""{"id": ${userId.value}}"""))
          .unsafeRunSync() shouldBe Right(AuthUser(userId, accessToken))
      }

    NotFound :: Unauthorized :: Forbidden :: Nil foreach { status =>
      s"return AuthenticationFailure if GitLab responds with $status" in new TestCase {

        mapResponse(status, Request(), Response())
          .unsafeRunSync() shouldBe Left(AuthenticationFailure)
      }
    }

    BadRequest :: ServiceUnavailable :: Nil foreach { status =>
      s"fail if GitLab responds with $status" in new TestCase {

        intercept[Exception] {
          mapResponse(status, Request(), Response())
            .unsafeRunSync()
        }
      }
    }

    s"fail if GitLab responds with malformed body" in new TestCase {

      intercept[Exception] {
        mapResponse(Status.Ok, Request(), Response().withEntity(json"{}"))
          .unsafeRunSync()
      }.getMessage should include(s"Could not decode JSON")
    }
  }

  private trait TestCase {
    val accessToken = accessTokens.generateOne
    val uri         = uri"user"
    val endpointName: NonEmptyString = "user"

    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabApiUrl  = GitLabUrl(externalServiceBaseUrl).apiV4
    val gitLabClient  = mock[GitLabClient[IO]]
    val authenticator = new GitLabAuthenticatorImpl[IO](gitLabClient)

    lazy val mapResponse =
      captureMapping(authenticator, gitLabClient)(
        _.authenticate(accessToken).unsafeRunSync(),
        Gen.const(Right(AuthUser(personGitLabIds.generateOne, accessToken)))
      )
  }

  private def `/api/v4/user`(header: Header.Raw) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get("/api/v4/user")
        .withHeader(header.name.toString, equalTo(header.value))
        .willReturn(response)
    }
  }
}
