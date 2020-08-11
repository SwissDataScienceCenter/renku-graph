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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users.Email
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.json.JsonOps._
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Json
import io.circe.literal._
import org.scalatest.matchers._
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

    "return info about a project with the given path - case when user cannot be found in GitLab" in new TestCase {
      forAll { path: model.projects.Path =>
        val project   = gitLabProjects(path).generateOne
        val creatorId = positiveInts().generateOne
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

    "return info about a project with the given path - case when user email is in the 'email' property" in new TestCase {
      val path      = projectPaths.generateOne
      val creator   = gitLabCreator(userEmails.generateSome).generateOne
      val project   = gitLabProjects(path).generateOne.copy(maybeCreator = creator.some)
      val creatorId = positiveInts().generateOne
      `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId.some).noSpaces)
      `/api/v4/users`(creatorId) returning okJson(userJsonWithEmailProperty(creator).noSpaces)

      finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project)
    }

    "return info about a project with the given path - case when user email is in the 'public_email' property" in new TestCase {
      val path      = projectPaths.generateOne
      val creator   = gitLabCreator(userEmails.generateSome).generateOne
      val project   = gitLabProjects(path).generateOne.copy(maybeCreator = creator.some)
      val creatorId = positiveInts().generateOne
      `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId.some).noSpaces)
      `/api/v4/users`(creatorId) returning okJson(userJsonWithPublicEmailProperty(creator).noSpaces)

      finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project)
    }

    "return info about a project with the given path - case when user email is in both 'email' and 'public_email' property" in new TestCase {
      val path      = projectPaths.generateOne
      val creator   = gitLabCreator(userEmails.generateSome).generateOne
      val project   = gitLabProjects(path).generateOne.copy(maybeCreator = creator.some)
      val creatorId = positiveInts().generateOne
      `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId.some).noSpaces)
      `/api/v4/users`(creatorId) returning okJson(
        (userJsonWithEmailProperty(creator) deepMerge userJsonWithPublicEmailProperty(
          creator.copy(maybeEmail = userEmails.generateSome)
        )).noSpaces
      )

      finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project)
    }

    "return info about a project with the given path - case when blank values in both 'email' and 'public_email' property" in new TestCase {
      val path      = projectPaths.generateOne
      val creator   = gitLabCreator(userEmails.generateNone).generateOne
      val project   = gitLabProjects(path).generateOne.copy(maybeCreator = creator.some)
      val creatorId = positiveInts().generateOne
      `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId.some).noSpaces)
      `/api/v4/users`(creatorId) returning okJson(
        userJsonWithEmailProperty(creator, emailBlank = true)
          .deepMerge(userJsonWithPublicEmailProperty(creator, emailBlank = true))
          .noSpaces
      )

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

  private def projectJson(project: GitLabProject, maybeCreatorId: Option[Int Refined Positive]): Json =
    json"""{
      "path_with_namespace": ${project.path.value},
      "created_at":          ${project.dateCreated.value}
    }"""
      .deepMerge(project.maybeParentPath.map { parentPath =>
        json"""{
          "forked_from_project": {
            "path_with_namespace": ${parentPath.value}
          }
        }"""
      } getOrElse Json.obj())
      .addIfDefined("creator_id" -> maybeCreatorId.map(_.value))

  private def userJsonWithEmailProperty(creator: GitLabCreator, emailBlank: Boolean = false): Json =
    json"""{
      "name":  ${creator.maybeName.map(_.value)},
      "email": ${creator.maybeEmail.toValue(emailBlank)}
    }"""

  private def userJsonWithPublicEmailProperty(creator: GitLabCreator, emailBlank: Boolean = false): Json =
    json"""{
      "name":         ${creator.maybeName.map(_.value)},
      "public_email": ${creator.maybeEmail.toValue(emailBlank)}
    }"""

  private implicit class EmailOps(maybeEmail: Option[Email]) {
    def toValue(emailBlank: Boolean): Option[String] =
      if (emailBlank) blankStrings().generateSome
      else maybeEmail.map(_.value)
  }

  private def `/api/v4/projects`(path: Path) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.toString)}")
        .willReturn(response)
    }
  }

  private def `/api/v4/users`(creatorId: Int Refined Positive) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/users/$creatorId")
        .willReturn(response)
    }
  }
}
