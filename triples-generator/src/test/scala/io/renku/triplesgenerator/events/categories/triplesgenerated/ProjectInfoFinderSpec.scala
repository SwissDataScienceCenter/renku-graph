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

package io.renku.triplesgenerator.events.categories.triplesgenerated

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
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{GitLabUrl, users}
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder._
import io.renku.interpreters.TestLogger
import io.renku.json.JsonOps._
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.http4s.Status.{Forbidden, ServiceUnavailable, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.{postfixOps, reflectiveCalls}

class ProjectInfoFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with TinyTypeEncoders {

  "findProjectInfo" should {

    "return info about a project with the given path" in new TestCase {
      forAll { (projectInfoRaw: GitLabProjectInfo, creator: ProjectMember) =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = creator.some)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)
        val (users, members) = projectInfo.members.splitAt(projectInfo.members.size / 2)
        `/api/v4/project/users`(projectInfo.path) returning okJson(users.asJson.noSpaces)
        `/api/v4/project/members`(projectInfo.path) returning okJson(members.asJson.noSpaces)

        finder.findProjectInfo(projectInfo.path).value.unsafeRunSync() shouldBe Some(projectInfo).asRight
      }
    }

    "return info without creator if it does not exist in GitLab" in new TestCase {
      forAll { (projectInfoRaw: GitLabProjectInfo, creator: ProjectMember) =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = creator.some)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        `/api/v4/users`(creator.gitLabId) returning notFound()
        val (users, members) = projectInfo.members.splitAt(projectInfo.members.size / 2)
        `/api/v4/project/users`(projectInfo.path) returning okJson(users.asJson.noSpaces)
        `/api/v4/project/members`(projectInfo.path) returning okJson(members.asJson.noSpaces)

        finder.findProjectInfo(projectInfo.path).value.unsafeRunSync() shouldBe Some(
          projectInfo.copy(maybeCreator = None)
        ).asRight
      }
    }

    "return info without creator if it's not returned in project info" in new TestCase {
      forAll { projectInfoRaw: GitLabProjectInfo =>
        val projectInfo = projectInfoRaw.copy(maybeCreator = None)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        val (users, members) = projectInfo.members.splitAt(projectInfo.members.size / 2)
        `/api/v4/project/users`(projectInfo.path) returning okJson(users.asJson.noSpaces)
        `/api/v4/project/members`(projectInfo.path) returning okJson(members.asJson.noSpaces)

        finder.findProjectInfo(projectInfo.path).value.unsafeRunSync() shouldBe Some(projectInfo).asRight
      }
    }

    "return no info when there's no project with the given path" in new TestCase {
      val path = projectPaths.generateOne
      `/api/v4/projects`(path) returning notFound()

      finder.findProjectInfo(path).value.unsafeRunSync() shouldBe None.asRight
    }

    "return info without members if they do not exist in GitLab" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne

      `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
      projectInfo.maybeCreator.foreach(creator =>
        `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)
      )
      `/api/v4/project/users`(projectInfo.path) returning okJson(projectInfo.members.asJson.noSpaces)
      `/api/v4/project/members`(projectInfo.path) returning notFound()

      finder.findProjectInfo(projectInfo.path).value.unsafeRunSync() shouldBe Some(projectInfo).asRight
    }

    "return info without users if they do not exist in GitLab" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne

      `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
      projectInfo.maybeCreator.foreach(creator =>
        `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)
      )
      `/api/v4/project/users`(projectInfo.path) returning notFound()
      `/api/v4/project/members`(projectInfo.path) returning okJson(projectInfo.members.asJson.noSpaces)

      finder.findProjectInfo(projectInfo.path).value.unsafeRunSync() shouldBe Some(projectInfo).asRight
    }

    "collect members from all the pages" in new TestCase {
      val allMembers  = projectMemberObjects.generateFixedSizeList(4)
      val projectInfo = gitLabProjectInfos.generateOne.copy(members = allMembers.toSet)

      `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
      projectInfo.maybeCreator.foreach(creator =>
        `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)
      )
      val (users, members) = projectInfo.members.splitAt(allMembers.size / 2)
      `/api/v4/project/users`(projectInfo.path) returning okJson(List(users.head).asJson.noSpaces)
        .withHeader("X-Next-Page", "2")
      `/api/v4/project/users`(projectInfo.path, maybePage = Some(2)) returning okJson(users.tail.asJson.noSpaces)
        .withHeader("X-Next-Page", "")
      `/api/v4/project/members`(projectInfo.path) returning okJson(List(members.head).asJson.noSpaces)
        .withHeader("X-Next-Page", "2")
      `/api/v4/project/members`(projectInfo.path, maybePage = Some(2)) returning okJson(members.tail.asJson.noSpaces)
        .withHeader("X-Next-Page", "")

      finder.findProjectInfo(projectInfo.path).value.unsafeRunSync() shouldBe Some(projectInfo).asRight
    }

    Set(
      "connection problem" -> aResponse().withFault(CONNECTION_RESET_BY_PEER),
      "client problem"     -> aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt),
      "ServiceUnavailable" -> aResponse().withStatus(ServiceUnavailable.code),
      "Forbidden"          -> aResponse().withStatus(Forbidden.code),
      "Unauthorized"       -> aResponse().withStatus(Unauthorized.code)
    ) foreach { case (problemName, response) =>
      s"return a Recoverable Failure for $problemName when fetching project info" in new TestCase {
        val path = projectPaths.generateOne
        `/api/v4/projects`(path) returning response

        val Left(failure) = finder.findProjectInfo(path).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }

      s"return a Recoverable Failure for $problemName when fetching creator" in new TestCase {
        val creator     = projectMemberObjects.generateOne
        val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = creator.some)

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        `/api/v4/users`(creator.gitLabId) returning response

        val Left(failure) = finder.findProjectInfo(projectInfo.path).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }

      s"return a Recoverable Failure for $problemName when fetching members" in new TestCase {
        val projectInfo = gitLabProjectInfos.generateOne

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        projectInfo.maybeCreator.foreach(creator =>
          `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)
        )
        `/api/v4/project/members`(projectInfo.path) returning response

        val Left(failure) = finder.findProjectInfo(projectInfo.path).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }

      s"return a Recoverable Failure for $problemName when fetching users" in new TestCase {
        val projectInfo = gitLabProjectInfos.generateOne

        `/api/v4/projects`(projectInfo.path) returning okJson(projectInfo.asJson.noSpaces)
        projectInfo.maybeCreator.foreach(creator =>
          `/api/v4/users`(creator.gitLabId) returning okJson(creator.asJson.noSpaces)
        )
        `/api/v4/project/users`(projectInfo.path) returning okJson(projectInfo.members.asJson.noSpaces)
        `/api/v4/project/members`(projectInfo.path) returning response

        val Left(failure) = finder.findProjectInfo(projectInfo.path).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }
    }
  }

  private lazy val requestTimeout = 2 seconds

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl = GitLabUrl(externalServiceBaseUrl).apiV4
    val finder = new ProjectInfoFinderImpl[IO](gitLabUrl,
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
      "path_with_namespace": ${project.path.value},
      "name":                ${project.name.value},
      "description": ${project.description.value},
      "created_at":          ${project.dateCreated.value},
      "visibility":          ${project.visibility.value}
    }"""
      .addIfDefined("forked_from_project" -> project.maybeParentPath)(parentPathEncoder)
      .addIfDefined("creator_id" -> project.maybeCreator.map(_.gitLabId))
  }

  private implicit lazy val memberEncoder: Encoder[ProjectMember] = Encoder.instance { member =>
    json"""{
      "id":       ${member.gitLabId},     
      "name":     ${member.name},
      "username": ${member.username}
    }"""
  }

  private def `/api/v4/projects`(path: Path) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.toString)}")
        .willReturn(response)
    }
  }

  private def `/api/v4/project/users`(path: Path, maybePage: Option[Int] = None) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.value)}/users${maybePage.map(p => s"?page=$p").getOrElse("")}")
        .willReturn(response)
    }
  }

  private def `/api/v4/project/members`(path: Path, maybePage: Option[Int] = None) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.value)}/members${maybePage.map(p => s"?page=$p").getOrElse("")}")
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
