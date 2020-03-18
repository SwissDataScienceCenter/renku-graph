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
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.Path
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Json
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class GitLabInfoFinderSpec extends WordSpec with ExternalServiceStubbing with ScalaCheckPropertyChecks {

  "findProject" should {

    "return info about a project with the given path" in new TestCase {
      forAll { path: model.projects.Path =>
        val project   = gitLabProjects(path).generateOne
        val creatorId = positiveInts().generateOne
        `/api/v4/projects`(path) returning okJson(projectJson(project, creatorId).noSpaces)
        `/api/v4/users`(creatorId) returning toResponse(creatorId, project.maybeCreator)

        finder.findProject(path)(maybeAccessToken).unsafeRunSync() shouldBe Some(project)
      }
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

  private def projectJson(project: GitLabProject, creatorId: Int Refined Positive): Json =
    json"""{
      "path_with_namespace": ${project.path.value},
      "created_at":          ${project.dateCreated.value},
      "creator_id":          ${creatorId.value}
    }""" deepMerge (project.maybeParentPath.map { parentPath =>
      json"""{
        "forked_from_project": {
          "path_with_namespace": ${parentPath.value}
        }
      }"""
    } getOrElse Json.obj())

  private def userJson(creatorId: Int Refined Positive, creator: GitLabCreator): Json =
    json"""{
      "name":         ${creator.maybeName.map(_.value)},
      "public_email": ${creator.maybeEmail.map(_.value)}
    }"""

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

  private def toResponse(creatorId:    Int Refined Positive,
                         maybeCreator: Option[GitLabCreator]): ResponseDefinitionBuilder =
    maybeCreator match {
      case Some(creator) => okJson(userJson(creatorId, creator).noSpaces)
      case _             => notFound()
    }
}
