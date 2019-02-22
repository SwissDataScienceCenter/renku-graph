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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.GraphCommonsGenerators._
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.test.FakeRequest

import scala.util.Try

class AccessTokenFinderSpec extends WordSpec {

  "findAccessToken" should {

    "return personal access token when PRIVATE-TOKEN is present" in new TestCase {

      val accessToken = personalAccessTokens.generateOne

      finder.findAccessToken(
        request.withHeaders("PRIVATE-TOKEN" -> accessToken.value)
      ) shouldBe context.pure(accessToken)
    }

    "return oauth access token when OAUTH-TOKEN is present in the header" in new TestCase {

      val accessToken = oauthAccessTokens.generateOne

      finder.findAccessToken(
        request.withHeaders("OAUTH-TOKEN" -> accessToken.value)
      ) shouldBe context.pure(accessToken)
    }

    "return oauth access token when both PRIVATE-TOKEN and OAUTH-TOKEN are present" in new TestCase {

      val oauthAccessToken    = oauthAccessTokens.generateOne
      val personalAccessToken = personalAccessTokens.generateOne

      finder.findAccessToken(
        request
          .withHeaders("OAUTH-TOKEN" -> oauthAccessToken.value)
          .withHeaders("PRIVATE-TOKEN" -> personalAccessToken.value)
      ) shouldBe context.pure(oauthAccessToken)
    }

    "fail with UNAUTHORIZED when neither PRIVATE-TOKEN nor OAUTH-TOKEN is not present" in new TestCase {
      finder.findAccessToken(request) shouldBe context.raiseError(UnauthorizedException)
    }

    "fail with UNAUTHORIZED when PRIVATE-TOKEN is invalid" in new TestCase {
      finder.findAccessToken(
        request.withHeaders("PRIVATE-TOKEN" -> "")
      ) shouldBe context.raiseError(UnauthorizedException)
    }

    "fail with UNAUTHORIZED when OAUTH-TOKEN is invalid" in new TestCase {
      finder.findAccessToken(
        request.withHeaders("OAUTH-TOKEN" -> "")
      ) shouldBe context.raiseError(UnauthorizedException)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val request = FakeRequest()

    val finder = new AccessTokenFinder[Try]()
  }
}
