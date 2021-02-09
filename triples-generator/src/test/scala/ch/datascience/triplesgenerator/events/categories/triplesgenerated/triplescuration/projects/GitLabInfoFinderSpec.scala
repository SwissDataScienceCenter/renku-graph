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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.json.JsonOps._
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class GitLabInfoFinderSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProject" should {

    "return info about a project with the given path" in new TestCase {
      forAll { path: model.projects.Path =>
        val creator   = gitLabCreator().generateOne
        val project   = gitLabProjects(path).generateOne.copy(maybeCreator = creator.some)
        val creatorId = userGitLabIds.generateOne
        `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId.some).noSpaces)
        `/api/v4/users`(creatorId) returning okJson(userJson(creator).noSpaces)

        finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project)
      }
    }

    "return info about a project with the given path - case when user cannot be found in GitLab" in new TestCase {
      forAll { path: model.projects.Path =>
        val project   = gitLabProjects(path).generateOne
        val creatorId = userGitLabIds.generateOne
        `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId.some).noSpaces)
        `/api/v4/users`(creatorId) returning notFound()

        finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project.copy(maybeCreator = None))
      }
    }

    "return info about a project with the given path - case when there's no 'creator_id'" in new TestCase {
      val path    = projectPaths.generateOne
      val project = gitLabProjects(path).generateOne.copy(maybeCreator = None)
      `/api/v4/projects`(path) returning okJson(projectJson(project, maybeCreatorId = None).noSpaces)

      finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project)
    }

    "return no info when there's no project with the given path" in new TestCase {
      val path = projectPaths.generateOne
      `/api/v4/projects`(path) returning notFound()

      finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe None
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val maybeAccessToken = accessTokens.generateOption

    val gitLabUrl = GitLabUrl(externalServiceBaseUrl)
    val finder    = new IOGitLabInfoFinder(gitLabUrl, Throttler.noThrottling, TestLogger())
  }

  private def projectJson(project: GitLabProject, maybeCreatorId: Option[users.GitLabId]): Json =
    json"""{
      "path_with_namespace": ${project.path.value},
      "name":                ${project.name.value},
      "created_at":          ${project.dateCreated.value},
      "visibility":          ${project.visibility.value}
    }"""
      .deepMerge(project.maybeParentPath.map { parentPath =>
        json"""{
          "forked_from_project": {
            "path_with_namespace": ${parentPath.value}
          }
        }"""
      } getOrElse Json.obj())
      .addIfDefined("creator_id" -> maybeCreatorId.map(_.value))

  private def userJson(creator: GitLabCreator, emailBlank: Boolean = false): Json =
    json"""{
      "id":   ${creator.gitLabId.value},     
      "name": ${creator.name.value}
    }"""

  private def `/api/v4/projects`(path: Path) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.toString)}")
        .willReturn(response)
    }
  }

  private def `/api/v4/users`(creatorId: users.GitLabId) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/users/$creatorId")
        .willReturn(response)
    }
  }
}
