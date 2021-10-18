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

package io.renku.tokenrepository.repository.association

import cats.effect.{ContextShift, IO, Timer}
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{oauthAccessTokens, personalAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class ProjectPathFinderSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "findProjectPath" should {

    "return fetched Project Path if service responds with OK and a valid body - personal access token case" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(projectJson))
      }

      pathFinder.findProjectPath(projectId, Some(personalAccessToken)).unsafeRunSync() shouldBe Some(projectPath)
    }

    "return fetched project info if service responds with OK and a valid body - oauth access token case" in new TestCase {

      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(projectJson))
      }

      pathFinder.findProjectPath(projectId, Some(oauthAccessToken)).unsafeRunSync() shouldBe Some(projectPath)
    }

    "return None if service responds with NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(notFound())
      }

      pathFinder.findProjectPath(projectId, None).unsafeRunSync() shouldBe None
    }

    "return None if remote client responds with UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(unauthorized())
      }

      pathFinder.findProjectPath(projectId, None).unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote client responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        pathFinder.findProjectPath(projectId, None).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId returned ${Status.BadRequest}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        pathFinder.findProjectPath(projectId, None).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/$projectId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val gitLabUrl   = GitLabUrl(externalServiceBaseUrl)
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val pathFinder = new IOProjectPathFinder(gitLabUrl, Throttler.noThrottling, TestLogger())

    lazy val projectJson: String = json"""{
      "id": ${projectId.value},
      "path_with_namespace": ${projectPath.value}
    }""".noSpaces
  }
}
