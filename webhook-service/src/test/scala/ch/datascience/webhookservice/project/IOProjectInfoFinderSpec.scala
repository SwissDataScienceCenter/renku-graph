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

package ch.datascience.webhookservice.project

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.GitLabConfigProvider.HostUrl
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.literal._
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
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(projectJson(Some(personalAccessToken))))
      }

      projectInfoFinder.findProjectInfo(projectId, Some(personalAccessToken)).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        projectVisibility,
        projectPath
      )
    }

    "return fetched project info if service responds with 200 and a valid body - oauth token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(projectJson(Some(oauthAccessToken))))
      }

      projectInfoFinder.findProjectInfo(projectId, Some(oauthAccessToken)).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        projectVisibility,
        projectPath
      )
    }

    "return fetched public project info if service responds with 200 and a valid body - no token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(okJson(projectJson(maybeAccessToken = None)))
      }

      projectInfoFinder.findProjectInfo(projectId, maybeAccessToken = None).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        ProjectVisibility.Public,
        projectPath
      )
    }

    "fail if fetching the the config fails" in new TestCase {
      val exception = exceptions.generateOne
      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, maybeAccessToken = None).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, maybeAccessToken = None).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(notFound().withBody("some error"))
      }

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, maybeAccessToken = None).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId returned ${Status.NotFound}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        projectInfoFinder.findProjectInfo(projectId, maybeAccessToken = None).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val gitLabUrl         = url(externalServiceBaseUrl)
    val projectId         = projectIds.generateOne
    val projectVisibility = projectVisibilities.generateOne
    val projectPath       = projectPaths.generateOne

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider.get _)
        .expects()
        .returning(returning)

    val projectInfoFinder = new IOProjectInfoFinder(configProvider, Throttler.noThrottling, TestLogger())

    def projectJson(maybeAccessToken: Option[AccessToken]): String = maybeAccessToken match {
      case Some(_) =>
        json"""{
          "id": ${projectId.value},
          "visibility": ${projectVisibility.value},
          "path_with_namespace": ${projectPath.value}
        }""".noSpaces
      case None =>
        json"""{
          "id": ${projectId.value},
          "path_with_namespace": ${projectPath.value}
        }""".noSpaces
    }
  }

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
}
