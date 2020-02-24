/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.projects.rest

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.{oauthAccessTokens, personalAccessTokens}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class GitLabProjectFinderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "findProject" should {

    "return fetched project info if service responds with OK and a valid body - personal access token case" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectPath.toString)}")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(projectJson))
      }

      projectFinder.findProject(projectPath, Some(personalAccessToken)).value.unsafeRunSync() shouldBe Some(
        project
      )
    }

    "return fetched project info if service responds with OK and a valid body - oauth access token case" in new TestCase {

      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectPath.toString)}")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(projectJson))
      }

      projectFinder.findProject(projectPath, Some(oauthAccessToken)).value.unsafeRunSync() shouldBe Some(
        project
      )
    }

    "return None if service responds with NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectPath.toString)}")
          .willReturn(notFound())
      }

      projectFinder.findProject(projectPath, None).value.unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote client responds with status different than OK or NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectPath.toString)}")
          .willReturn(unauthorized().withBody("some error"))
      }

      intercept[Exception] {
        projectFinder.findProject(projectPath, None).value.unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/${urlEncode(projectPath.toString)} returned ${Status.Unauthorized}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(projectPath.toString)}")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        projectFinder.findProject(projectPath, None).value.unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/${urlEncode(projectPath.toString)} returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val gitLabUrl   = GitLabUrl(externalServiceBaseUrl)
    val projectPath = projectPaths.generateOne
    val project     = gitLabProjects.generateOne

    val projectFinder = new IOGitLabProjectFinder(gitLabUrl, Throttler.noThrottling, TestLogger())

    lazy val projectJson: String = json"""{
      "id": ${project.id.value},
      "visibility": ${project.visibility.value},
      "ssh_url_to_repo": ${project.urls.ssh.value},
      "http_url_to_repo": ${project.urls.http.value}
    }""".noSpaces
  }
}
