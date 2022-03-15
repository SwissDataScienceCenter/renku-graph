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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.blankStrings
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.entities.Project.GitLabProjectInfo
import io.renku.graph.model.entities.Project.ProjectMember.ProjectMemberNoEmail
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{GitLabUrl, persons, projects}
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder._
import io.renku.interpreters.TestLogger
import io.renku.json.JsonOps._
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError._
import org.http4s.Status.{BadGateway, Forbidden, ServiceUnavailable, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.reflectiveCalls

class ProjectFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with TinyTypeEncoders {

  "findProject" should {

    "fetch info about the project with the given path from GitLab" in new TestCase {
      forAll { (projectInfoRaw: GitLabProjectInfo, creator: ProjectMemberNoEmail) =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = creator.some)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)

        finder.findProject(projectInfo.path).value.unsafeRunSync() shouldBe
          projectInfo.copy(members = Set.empty, maybeCreator = creator.some).some.asRight
      }
    }

    "fetch info about the project without creator if it does not exist" in new TestCase {
      forAll { (projectInfoRaw: GitLabProjectInfo, creator: ProjectMemberNoEmail) =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = creator.some)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        `/api/v4/users`(creator.gitLabId) returning notFound()

        finder.findProject(projectInfo.path).value.unsafeRunSync() shouldBe
          projectInfo.copy(maybeCreator = None, members = Set.empty).some.asRight
      }
    }

    "default to visibility Public if not returned (quite likely due to invalid token)" in new TestCase {
      // It should be safe as for non-public repos and invalid/no token we'd not get any response
      val projectInfo = gitLabProjectInfos.generateOne
        .copy(maybeCreator = None, visibility = projects.Visibility.Public)

      val json = projectInfo.asJson.hcursor
        .downField("visibility")
        .delete
        .top
        .getOrElse(fail("Deleting visibility failed"))
      `/api/v4/projects`(projectInfo.path) returning okJson(json.noSpaces)

      finder.findProject(projectInfo.path).value.unsafeRunSync() shouldBe
        projectInfo.copy(maybeCreator = None, members = Set.empty).some.asRight
    }

    "return info without creator if it's not returned from the GET projects/:id" in new TestCase {
      forAll { projectInfoRaw: GitLabProjectInfo =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = None)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)

        finder.findProject(projectInfo.path).value.unsafeRunSync() shouldBe
          projectInfo.copy(maybeCreator = None, members = Set.empty).some.asRight
      }
    }

    "return no info when there's no project with the given path" in new TestCase {
      val path = projectPaths.generateOne
      `/api/v4/projects`(path) returning notFound()

      finder.findProject(path).value.unsafeRunSync() shouldBe None.asRight
    }

    val shouldBeLogWorthy = (failure: ProcessingRecoverableError) => failure shouldBe a[LogWorthyRecoverableError]
    val shouldBeAuth      = (failure: ProcessingRecoverableError) => failure shouldBe a[AuthRecoverableError]

    forAll(
      Table(
        ("Problem Name", "Failing Response", "Expected Failure type"),
        ("connection problem", aResponse().withFault(CONNECTION_RESET_BY_PEER), shouldBeLogWorthy),
        ("client problem", aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt), shouldBeLogWorthy),
        ("BadGateway", aResponse().withStatus(BadGateway.code), shouldBeLogWorthy),
        ("ServiceUnavailable", aResponse().withStatus(ServiceUnavailable.code), shouldBeLogWorthy),
        ("Forbidden", aResponse().withStatus(Forbidden.code), shouldBeAuth),
        ("Unauthorized", aResponse().withStatus(Unauthorized.code), shouldBeAuth)
      )
    ) { case (problemName, response, failureTypeAssertion) =>
      s"return a Recoverable Failure for $problemName when fetching project info" in new TestCase {
        val path = projectPaths.generateOne
        `/api/v4/projects`(path) returning response

        val Left(failure) = finder.findProject(path).value.unsafeRunSync()
        failureTypeAssertion(failure)
      }

      s"return a Recoverable Failure for $problemName when fetching creator" in new TestCase {
        val creator     = projectMembersNoEmail.generateOne
        val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = creator.some)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        `/api/v4/users`(creator.gitLabId) returning response

        val Left(failure) = finder.findProject(projectInfo.path).value.unsafeRunSync()
        failureTypeAssertion(failure)
      }
    }
  }

  private lazy val requestTimeout = 2 seconds

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl = GitLabUrl(externalServiceBaseUrl).apiV4
    val finder = new ProjectFinderImpl[IO](gitLabUrl,
                                           Throttler.noThrottling,
                                           retryInterval = 100 millis,
                                           maxRetries = 1,
                                           requestTimeoutOverride = Some(requestTimeout)
    )
  }

  private implicit lazy val projectInfoEncoder: Encoder[GitLabProjectInfo] = Encoder.instance { project =>
    val parentPathEncoder: Encoder[Path] = Encoder.instance(path => json"""{
      "path_with_namespace": $path
    }""")

    json"""{
      "id":                  ${project.id},
      "path_with_namespace": ${project.path},
      "name":                ${project.name},
      "created_at":          ${project.dateCreated},
      "visibility":          ${project.visibility},
      "tag_list":            ${project.keywords.map(_.value) + blankStrings().generateOne}
    }"""
      .addIfDefined("forked_from_project" -> project.maybeParentPath)(parentPathEncoder)
      .addIfDefined("creator_id" -> project.maybeCreator.map(_.gitLabId))
      .addIfDefined("description" -> project.maybeDescription.map(_.value))
  }

  private implicit lazy val memberEncoder: Encoder[ProjectMemberNoEmail] = Encoder.instance { member =>
    json"""{
      "id":       ${member.gitLabId},     
      "name":     ${member.name},
      "username": ${member.username}
    }"""
  }

  private def `/api/v4/projects`(path: Path)(implicit maybeAccessToken: Option[AccessToken]) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.toString)}")
        .withAccessToken(maybeAccessToken)
        .willReturn(response)
    }
  }

  private def `/api/v4/users`(creatorId: persons.GitLabId)(implicit maybeAccessToken: Option[AccessToken]) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/users/$creatorId")
        .withAccessToken(maybeAccessToken)
        .willReturn(response)
    }
  }
}
