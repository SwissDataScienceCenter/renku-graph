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

package io.renku.knowledgegraph.projects.rest

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{oauthAccessTokens, personalAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.projects.model.Permissions
import io.renku.knowledgegraph.projects.model.Permissions._
import io.renku.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import io.renku.knowledgegraph.projects.rest.ProjectsGenerators._
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GitLabProjectFinderSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "findProject" should {

    "return fetched project info if service responds with OK and a valid body - personal access token case" in new TestCase {
      forAll { (path: model.projects.Path, accessToken: PersonalAccessToken, project: GitLabProject) =>
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}?statistics=true")
            .withHeader("PRIVATE-TOKEN", equalTo(accessToken.value))
            .willReturn(okJson(projectJson(project).noSpaces))
        }

        projectFinder.findProject(path, Some(accessToken)).value.unsafeRunSync() shouldBe Some(project)
      }
    }

    "return fetched project info if service responds with OK and a valid body - oauth access token case" in new TestCase {
      forAll { (path: model.projects.Path, accessToken: OAuthAccessToken, project: GitLabProject) =>
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}?statistics=true")
            .withHeader("Authorization", equalTo(s"Bearer ${accessToken.value}"))
            .willReturn(okJson(projectJson(project).noSpaces))
        }

        projectFinder.findProject(path, Some(accessToken)).value.unsafeRunSync() shouldBe Some(project)
      }
    }

    "return fetched project info with no readme if readme_url in remote is blank" in new TestCase {
      val path          = projectPaths.generateOne
      val gitLabProject = gitLabProjects.generateOne
      val project       = gitLabProject.copy(urls = gitLabProject.urls.copy(maybeReadme = None))

      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}?statistics=true")
          .willReturn(
            okJson(
              projectJson(project).noSpaces
            )
          )
      }

      projectFinder.findProject(path, maybeAccessToken = None).value.unsafeRunSync() shouldBe Some(project)
    }

    "return None if service responds with NOT_FOUND" in new TestCase {

      val path = projectPaths.generateOne
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}?statistics=true")
          .willReturn(notFound())
      }

      projectFinder.findProject(path, None).value.unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote client responds with status different than OK or NOT_FOUND" in new TestCase {

      val path = projectPaths.generateOne
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}?statistics=true")
          .willReturn(unauthorized().withBody("some error"))
      }

      intercept[Exception] {
        projectFinder.findProject(path, None).value.unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/${urlEncode(path.toString)}?statistics=true returned ${Status.Unauthorized}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      val path = projectPaths.generateOne
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}?statistics=true")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        projectFinder.findProject(path, None).value.unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/${urlEncode(path.toString)}?statistics=true returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl     = GitLabUrl(externalServiceBaseUrl)
    val projectFinder = new GitLabProjectFinderImpl[IO](gitLabUrl, Throttler.noThrottling)
  }

  private def projectJson(project: GitLabProject): Json = json"""{
    "id":               ${project.id.value},
    "visibility":       ${project.visibility.value},
    "ssh_url_to_repo":  ${project.urls.ssh.value},
    "http_url_to_repo": ${project.urls.http.value},
    "web_url":          ${project.urls.web.value},
    "readme_url":       ${project.urls.maybeReadme.map(_.value)},
    "forks_count":      ${project.forksCount.value},
    "star_count":       ${project.starsCount.value},
    "last_activity_at": ${project.updatedAt.value},
    "permissions":      ${toJson(project.permissions)},
    "statistics": {
      "commit_count":       ${project.statistics.commitsCount.value},
      "storage_size":       ${project.statistics.storageSize.value},
      "repository_size":    ${project.statistics.repositorySize.value},
      "lfs_objects_size":   ${project.statistics.lsfObjectsSize.value},
      "job_artifacts_size": ${project.statistics.jobArtifactsSize.value}
    }
  }"""

  private lazy val toJson: Permissions => Json = {
    case ProjectAndGroupPermissions(project, group) => json"""{
      "project_access": ${toJson(project)},
      "group_access":   ${toJson(group)}
    }"""
    case ProjectPermissions(project) => json"""{
      "project_access": ${toJson(project)},
      "group_access":   ${Json.Null}
    }"""
    case GroupPermissions(group) => json"""{
      "project_access": ${Json.Null},
      "group_access":   ${toJson(group)}
    }"""
  }

  private def toJson(accessLevel: AccessLevel): Json = json"""{
    "access_level": ${accessLevel.value.value}
  }"""
}
