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

package io.renku.triplesgenerator.events.categories.membersync

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

import Generators._
import cats.effect.{ContextShift, IO, Timer}
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import org.http4s.Status.{Forbidden, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global

class GitLabProjectMembersFinderSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProjectMembers" should {

    "return a set of project members and users" in new TestCase {

      forAll { (gitLabProjectUsers: List[GitLabProjectMember], gitLabProjectMembers: List[GitLabProjectMember]) =>
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
            .willReturn(okJson(gitLabProjectUsers.asJson.noSpaces))
        }
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
            .willReturn(okJson(gitLabProjectMembers.asJson.noSpaces))
        }

        finder
          .findProjectMembers(path)
          .unsafeRunSync() shouldBe (gitLabProjectUsers.toSet ++ gitLabProjectMembers.toSet)
      }
    }

    "collect users from paged results" in new TestCase {
      val projectUsers   = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList
      val projectMembers = gitLabProjectMembers.generateNonEmptyList(minElements = 2).toList

      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
          .willReturn(okJson(List(projectUsers.head).asJson.noSpaces).withHeader("X-Next-Page", "2"))
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
          .willReturn(okJson(List(projectMembers.head).asJson.noSpaces).withHeader("X-Next-Page", "2"))
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/users?page=2")
          .willReturn(okJson(projectUsers.tail.asJson.noSpaces).withHeader("X-Next-Page", ""))
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/members?page=2")
          .willReturn(okJson(projectMembers.tail.asJson.noSpaces).withHeader("X-Next-Page", ""))
      }

      finder
        .findProjectMembers(path)
        .unsafeRunSync() shouldBe (projectUsers.toSet ++ projectMembers.toSet)
    }

    "return an empty list when service responds with NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/users")
          .willReturn(notFound())
      }
      stubFor {
        get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
          .willReturn(notFound())
      }

      finder.findProjectMembers(path).unsafeRunSync() shouldBe Set.empty
    }

    Forbidden +: Unauthorized +: Nil foreach { status =>
      s"fail when service responds with $status" in new TestCase {
        stubFor {
          get(s"/api/v4/projects/${urlEncode(path.toString)}/members")
            .willReturn(aResponse.withStatus(status.code))
        }

        intercept[Exception] {
          finder.findProjectMembers(path).unsafeRunSync()
        }
      }
    }
  }

  private implicit lazy val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit lazy val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val path = projectPaths.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private val gitLabUrl = GitLabUrl(externalServiceBaseUrl)
    val finder            = new IOGitLabProjectMembersFinder(gitLabUrl.apiV4, Throttler.noThrottling, TestLogger())
  }

  private implicit val projectMemberEncoder: Encoder[GitLabProjectMember] = Encoder.instance[GitLabProjectMember] {
    member =>
      json"""{
      "id":        ${member.gitLabId.value},
      "username":  ${member.name.value},
      "name":      ${member.name.value}
    }"""
  }
}
