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

package ch.datascience.webhookservice.tokenrepository

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.tokenrepository.TokenRepositoryUrl
import ch.datascience.http.ErrorMessage
import ch.datascience.http.ErrorMessage._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class AccessTokenAssociatorImplSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "associate" should {

    personalAccessTokens.generateOne +: oauthAccessTokens.generateOne +: Nil foreach { accessToken =>
      s"succeed if association projectId with a ${accessToken.getClass.getSimpleName} on a remote is successful" in new TestCase {

        stubFor {
          put(s"/projects/$projectId/tokens")
            .withRequestBody(equalToJson(toJson(accessToken)))
            .willReturn(noContent())
        }

        associator.associate(projectId, accessToken).unsafeRunSync() shouldBe ((): Unit)
      }
    }

    "return an Exception if remote client responds with a status other than NO_CONTENT" in new TestCase {

      val accessToken = accessTokens.generateOne

      val responseBody = ErrorMessage("some error").asJson.noSpaces
      stubFor {
        put(s"/projects/$projectId/tokens")
          .withRequestBody(equalToJson(toJson(accessToken)))
          .willReturn(badRequest.withBody(responseBody))
      }

      intercept[Exception] {
        associator.associate(projectId, accessToken).unsafeRunSync()
      }.getMessage shouldBe s"PUT $tokenRepositoryUrl/projects/$projectId/tokens returned ${Status.BadRequest}; body: $responseBody"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val tokenRepositoryUrl = TokenRepositoryUrl(externalServiceBaseUrl)
    val projectId          = projectIds.generateOne

    val associator = new AccessTokenAssociatorImpl[IO](tokenRepositoryUrl, TestLogger())
  }

  private lazy val toJson: AccessToken => String = {
    case PersonalAccessToken(token) => json"""{"personalAccessToken": $token}""".noSpaces
    case OAuthAccessToken(token)    => json"""{"oauthAccessToken": $token}""".noSpaces
  }
}
