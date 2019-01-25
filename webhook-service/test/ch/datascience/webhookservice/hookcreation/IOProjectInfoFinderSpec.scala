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

import cats.effect.{IO, Sync}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.GitLabConfig.HostUrl
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.model.{ProjectInfo, ProjectOwner}
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOProjectInfoFinderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "findProjectInfo" should {

    "return fetched project info if service responds with 200 and a valid body - personal access token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(okJson(projectJson))
      }

      projectInfoFinder.findProjectInfo(projectId, personalAccessToken).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        projectPath,
        ProjectOwner(userId)
      )
    }

    "return fetched project info if service responds with 200 and a valid body - oauth token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("Authorization", equalTo(s"Bearer $oauthAccessToken"))
          .willReturn(okJson(projectJson))
      }

      projectInfoFinder.findProjectInfo(projectId, oauthAccessToken).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        projectPath,
        ProjectOwner(userId)
      )
    }

    "fail if fetching the the config fails" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne
      val exception           = exceptions.generateOne

      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, personalAccessToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(notFound().withBody("some error"))
      }

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId returned ${Status.NotFound}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private trait TestCase {
    val gitLabUrl   = url(externalServiceBaseUrl)
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne
    val userId      = userIds.generateOne

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider
        .get()(_: Sync[IO]))
        .expects(*)
        .returning(returning)

    val projectInfoFinder = new IOProjectInfoFinder(configProvider)

    lazy val projectJson: String = Json
      .obj(
        "id"                  -> Json.fromInt(projectId.value),
        "path_with_namespace" -> Json.fromString(projectPath.value),
        "owner" -> Json.obj(
          "id" -> Json.fromInt(userId.value)
        )
      )
      .toString()
  }

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
}
