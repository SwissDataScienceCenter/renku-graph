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

package io.renku.webhookservice.hookfetcher

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators.{hookIdAndUrls, projectHookUrls}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectHookFetcherSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with IOSpec {

  "fetchProjectHooks" should {

    "return the list of hooks of the project - personal access token case" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne
      val idAndUrls           = hookIdAndUrls.toGeneratorOfNonEmptyList(2).generateOne
      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(idAndUrls.toList.asJson.noSpaces))
      }

      fetcher.fetchProjectHooks(projectId, personalAccessToken).unsafeRunSync() shouldBe idAndUrls.toList
    }
    "return the list of hooks of the project - oauth token case" in new TestCase {

      val oauthAccessToken = oauthAccessTokens.generateOne
      val idAndUrls        = hookIdAndUrls.toGeneratorOfNonEmptyList(2).generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(idAndUrls.toList.asJson.noSpaces))
      }

      fetcher.fetchProjectHooks(projectId, oauthAccessToken).unsafeRunSync() shouldBe idAndUrls.toList
    }
    "decode the list of hooks if the id is sent as a number" in new TestCase {

      val oauthAccessToken = oauthAccessTokens.generateOne
      val id               = 123
      val url              = projectHookUrls.generateOne
      val result           = HookIdAndUrl(id.toString, url)
      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(s"""[{"id":$id, "url":"${url.value}" }]"""))
      }

      fetcher.fetchProjectHooks(projectId, oauthAccessToken).unsafeRunSync() shouldBe List(result)
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        fetcher.fetchProjectHooks(projectId, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(serviceUnavailable().withBody("some error"))
      }

      intercept[Exception] {
        fetcher.fetchProjectHooks(projectId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.ServiceUnavailable}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        fetcher.fetchProjectHooks(projectId, personalAccessToken).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabUrl = GitLabUrl(externalServiceBaseUrl)
    val projectId = projectIds.generateOne

    val fetcher = new ProjectHookFetcherImpl[IO](gitLabUrl, Throttler.noThrottling)
    implicit val idsAndUrlsEncoder: Encoder[HookIdAndUrl] = Encoder.instance { idAndUrl =>
      Json.obj(
        "id"  -> idAndUrl.id.asJson,
        "url" -> idAndUrl.url.value.asJson
      )
    }
  }
}
