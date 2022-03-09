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
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.entities.Project.ProjectMember.ProjectMemberNoEmail
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.http4s.Status.{BadGateway, Forbidden, ServiceUnavailable, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.Random

class ProjectMembersFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with TinyTypeEncoders {

  "findProject" should {

    "fetch and merge project users and members" in new TestCase {
      forAll { (members: Set[ProjectMemberNoEmail], users: Set[ProjectMemberNoEmail]) =>
        `/api/v4/project/users`(projectPath) returning okJson(users.asJson.noSpaces)
        `/api/v4/project/members`(projectPath) returning okJson(members.asJson.noSpaces)

        finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe (members ++ users).asRight
      }
    }

    "collect members from all the pages" in new TestCase {
      val allMembers = projectMembersNoEmail.generateFixedSizeList(4)

      val (users, members) = allMembers.splitAt(allMembers.size / 2)
      `/api/v4/project/users`(projectPath) returning okJson(List(users.head).asJson.noSpaces)
        .withHeader("X-Next-Page", "2")
      `/api/v4/project/users`(projectPath, maybePage = Some(2)) returning okJson(users.tail.asJson.noSpaces)
        .withHeader("X-Next-Page", "")
      `/api/v4/project/members`(projectPath) returning okJson(List(members.head).asJson.noSpaces)
        .withHeader("X-Next-Page", "2")
      `/api/v4/project/members`(projectPath, maybePage = Some(2)) returning okJson(members.tail.asJson.noSpaces)
        .withHeader("X-Next-Page", "")

      finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe allMembers.toSet.asRight
    }

    "return members even if one of the endpoints responds with NOT_FOUND" in new TestCase {
      val members = projectMembersNoEmail.generateSet()
      if (Random.nextBoolean()) {
        `/api/v4/project/users`(projectPath) returning notFound()
        `/api/v4/project/members`(projectPath) returning okJson(members.asJson.noSpaces)
      } else {
        `/api/v4/project/users`(projectPath) returning okJson(members.asJson.noSpaces)
        `/api/v4/project/members`(projectPath) returning notFound()
      }

      finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe members.asRight
    }

    "return an empty set if one of the endpoints returns an empty list" in new TestCase {
      val members = projectMembersNoEmail.generateSet()
      if (Random.nextBoolean()) {
        `/api/v4/project/users`(projectPath) returning okJson(Json.arr().noSpaces)
        `/api/v4/project/members`(projectPath) returning okJson(members.asJson.noSpaces)
      } else {
        `/api/v4/project/users`(projectPath) returning okJson(members.asJson.noSpaces)
        `/api/v4/project/members`(projectPath) returning okJson(Json.arr().noSpaces)
      }

      finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe members.asRight
    }

    "return an empty list if both of the endpoints returns an empty list" in new TestCase {
      `/api/v4/project/users`(projectPath) returning okJson(Json.arr().noSpaces)
      `/api/v4/project/members`(projectPath) returning okJson(Json.arr().noSpaces)

      finder.findProjectMembers(projectPath).value.unsafeRunSync() shouldBe Set.empty.asRight
    }

    Set(
      "connection problem" -> aResponse().withFault(CONNECTION_RESET_BY_PEER),
      "client problem"     -> aResponse().withFixedDelay((requestTimeout.toMillis + 500).toInt),
      "BadGateway"         -> aResponse().withStatus(BadGateway.code),
      "ServiceUnavailable" -> aResponse().withStatus(ServiceUnavailable.code),
      "Forbidden"          -> aResponse().withStatus(Forbidden.code),
      "Unauthorized"       -> aResponse().withStatus(Unauthorized.code)
    ) foreach { case (problemName, response) =>
      s"return a Recoverable Failure for $problemName when fetching project members or users" in new TestCase {
        if (Random.nextBoolean()) {
          `/api/v4/project/users`(projectPath) returning okJson(Json.arr().noSpaces)
          `/api/v4/project/members`(projectPath) returning response
        } else {
          `/api/v4/project/users`(projectPath) returning response
          `/api/v4/project/members`(projectPath) returning okJson(Json.arr().noSpaces)
        }

        val Left(failure) = finder.findProjectMembers(projectPath).value.unsafeRunSync()
        failure shouldBe a[ProcessingRecoverableError]
      }
    }
  }

  private lazy val requestTimeout = 2 seconds

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val projectPath = projectPaths.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl = GitLabUrl(externalServiceBaseUrl).apiV4
    val finder = new ProjectMembersFinderImpl[IO](gitLabUrl,
                                                  Throttler.noThrottling,
                                                  retryInterval = 100 millis,
                                                  maxRetries = 1,
                                                  requestTimeoutOverride = Some(requestTimeout)
    )
  }

  private implicit lazy val memberEncoder: Encoder[ProjectMemberNoEmail] = Encoder.instance { member =>
    json"""{
      "id":       ${member.gitLabId},     
      "name":     ${member.name},
      "username": ${member.username}
    }"""
  }

  private def `/api/v4/project/users`(path: Path, maybePage: Option[Int] = None)(implicit
      maybeAccessToken:                     Option[AccessToken]
  ) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.value)}/users${maybePage.map(p => s"?page=$p").getOrElse("")}")
        .withAccessToken(maybeAccessToken)
        .willReturn(response)
    }
  }

  private def `/api/v4/project/members`(path: Path, maybePage: Option[Int] = None)(implicit
      maybeAccessToken:                       Option[AccessToken]
  ) = new {
    def returning(response: ResponseDefinitionBuilder) = stubFor {
      get(s"/api/v4/projects/${urlEncode(path.value)}/members${maybePage.map(p => s"?page=$p").getOrElse("")}")
        .withAccessToken(maybeAccessToken)
        .willReturn(response)
    }
  }
}
