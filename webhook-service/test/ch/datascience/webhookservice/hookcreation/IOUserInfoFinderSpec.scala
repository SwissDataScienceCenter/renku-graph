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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.GraphCommonsGenerators._
import ch.datascience.graph.model.events.UserId
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.IOContextShift
import ch.datascience.webhookservice.config.GitLabConfig.HostUrl
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.HookCreationGenerators._
import ch.datascience.webhookservice.hookcreation.UserInfoFinder.UserInfo
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOUserInfoFinderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "findUserInfo" should {

    "return user info if remote responds with OK and valid body - personal access token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      val userInfo = userInfoWith(userId)
      stubFor {
        get(s"/api/v4/users/$userId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(userInfo.toJson))
      }

      userInfoFinder.findUserInfo(userId, personalAccessToken).unsafeRunSync() shouldBe userInfo
    }

    "return user info if remote responds with OK and valid body - oauth access token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      val userInfo = userInfoWith(userId)
      stubFor {
        get(s"/api/v4/users/$userId")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(userInfo.toJson))
      }

      userInfoFinder.findUserInfo(userId, oauthAccessToken).unsafeRunSync() shouldBe userInfo
    }

    "fail if fetching the the config fails" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne
      val exception           = exceptions.generateOne

      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        userInfoFinder.findUserInfo(userId, personalAccessToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/$userId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        userInfoFinder.findUserInfo(userId, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/$userId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(notFound().withBody("some error"))
      }

      intercept[Exception] {
        userInfoFinder.findUserInfo(userId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/users/$userId returned ${Status.NotFound}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/$userId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        userInfoFinder.findUserInfo(userId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/users/$userId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs: IOContextShift = new IOContextShift(global)

  private trait TestCase {
    val gitLabUrl   = url(externalServiceBaseUrl)
    val userId      = userIds.generateOne
    val accessToken = accessTokens.generateOne

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider.get _)
        .expects()
        .returning(returning)

    val userInfoFinder = new IOUserInfoFinder(configProvider)
  }

  private implicit class UserInfoOps(userInfo: UserInfo) {
    lazy val toJson: String = Json
      .obj(
        "id"           -> Json.fromInt(userInfo.userId.value),
        "username"     -> Json.fromString(userInfo.username.value),
        "email"        -> Json.fromString(emails.generateOne.value),
        "public_email" -> Json.fromString(emails.generateOne.value)
      )
      .noSpaces
  }

  private def userInfoWith(userId: UserId) =
    userInfos.generateOne
      .copy(userId = userId)

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
}
