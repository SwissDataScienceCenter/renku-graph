/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.effect.{ContextShift, IO}
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, oauthAccessTokens, personalAccessTokens}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, ProjectId, UserId}
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.GitLabConfigProvider.HostUrl
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.LatestPushEventFetcher.PushEventInfo
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOLatestPushEventFetcherSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "findLatestPushEvent" should {

    "return latest fetched PushEvent if remote responds with OK and valid body - personal access token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(pushEvents(projectId, commitsIdsList)))
      }

      pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync() shouldBe Some(
        PushEventInfo(
          projectId = projectId,
          authorId  = UserId(5),
          commitTo  = commitsIdsList.head
        )
      )
    }

    "return latest fetched PushEvent if remote responds with OK and valid body - oauth token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(pushEvents(projectId, commitsIdsList)))
      }

      pushEventFetcher.fetchLatestPushEvent(projectId, oauthAccessToken).unsafeRunSync() shouldBe Some(
        PushEventInfo(
          projectId = projectId,
          authorId  = UserId(5),
          commitTo  = commitsIdsList.head
        )
      )
    }

    "return None if remote responds with OK and no events" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson("[]"))
      }

      pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync() shouldBe None
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(notFound())
      }

      pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync() shouldBe None
    }

    "fail if fetching the the config fails" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne
      val exception           = exceptions.generateOne

      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/events?action=pushed returned ${Status.BadRequest}; body: some error"
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/events?action=pushed")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        pushEventFetcher.fetchLatestPushEvent(projectId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/events?action=pushed returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val gitLabUrl      = url(externalServiceBaseUrl)
    val projectId      = projectIds.generateOne
    val accessToken    = accessTokens.generateOne
    val commitsIdsList = Gen.listOfN(positiveInts(max = 3).generateOne, commitIds).generateOne

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider.get _)
        .expects()
        .returning(returning)

    val pushEventFetcher = new IOLatestPushEventFetcher(configProvider)
  }

  private def pushEvents(projectId: ProjectId, commitIds: Seq[CommitId]) =
    Json.arr(commitIds map pushEvent(projectId): _*).noSpaces

  private def pushEvent(projectId: ProjectId)(commitId: CommitId) = Json.obj(
    "project_id" -> Json.fromInt(projectId.value),
    "author_id"  -> Json.fromInt(5),
    "push_data" -> Json.obj(
      "commit_to" -> Json.fromString(commitId.value)
    )
  )

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
}
