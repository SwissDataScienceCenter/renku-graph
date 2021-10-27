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

package io.renku.graph.http.server.security

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.userGitLabIds
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.server.security._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Header
import org.http4s.Status._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls

class GitLabAuthenticatorSpec extends AnyWordSpec with IOSpec with should.Matchers with ExternalServiceStubbing {

  "authenticate" should {

    "use the given access token to call GitLab's GET /user endpoint " +
      "and return authorized user if GitLab responds with OK" in new TestCase {

        val userId = userGitLabIds.generateOne
        `/api/v4/user`(accessToken.toHeader).returning(okJson(json"""{"id": ${userId.value}}""".noSpaces))

        authenticator.authenticate(accessToken).unsafeRunSync() shouldBe Right(AuthUser(userId, accessToken))
      }

    NotFound :: Unauthorized :: Forbidden :: Nil foreach { status =>
      s"return AuthenticationFailure if GitLab responds with $status" in new TestCase {

        `/api/v4/user`(accessToken.toHeader)
          .returning(aResponse().withStatus(status.code))

        authenticator.authenticate(accessToken).unsafeRunSync() shouldBe Left(AuthenticationFailure)
      }
    }

    BadRequest :: ServiceUnavailable :: Nil foreach { status =>
      s"fail if GitLab responds with $status" in new TestCase {

        val responseBody = sentences().generateOne
        `/api/v4/user`(accessToken.toHeader)
          .returning(aResponse().withStatus(status.code).withBody(responseBody.toString()))

        intercept[Exception] {
          authenticator.authenticate(accessToken).unsafeRunSync()
        }.getMessage shouldBe s"GET $gitLabApiUrl/user returned $status; body: $responseBody"
      }
    }

    s"fail if GitLab responds with malformed body" in new TestCase {

      `/api/v4/user`(accessToken.toHeader)
        .returning(okJson(sentences().generateOne.toString()))

      intercept[Exception] {
        authenticator.authenticate(accessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabApiUrl/user returned $Ok; error: Malformed message body: Invalid JSON"
    }
  }

  private trait TestCase {
    val accessToken = accessTokens.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabApiUrl  = GitLabUrl(externalServiceBaseUrl).apiV4
    val authenticator = new GitLabAuthenticatorImpl[IO](gitLabApiUrl, Throttler.noThrottling)
  }

  private def `/api/v4/user`(header: Header.Raw) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get("/api/v4/user")
        .withHeader(header.name.toString, equalTo(header.value))
        .willReturn(response)
    }
  }
}
