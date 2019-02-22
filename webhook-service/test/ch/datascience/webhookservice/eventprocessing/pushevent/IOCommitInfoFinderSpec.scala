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

package ch.datascience.webhookservice.eventprocessing.pushevent

import java.time.{LocalDateTime, ZoneOffset}

import cats.effect.{IO, Sync}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.generators.CommonsTypesGenerators._
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.GitLabConfig._
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.parser._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOCommitInfoFinderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "findCommitInfo" should {

    "fetch commit info from the configured url " +
      "and return CommitInfo if OK returned with valid body" in new TestCase {

      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(okJson(responseJson.toString()))
      }

      finder.findCommitInfo(projectId, commitId).unsafeRunSync() shouldBe CommitInfo(
        id            = commitId,
        message       = commitMessage,
        committedDate = committedDate,
        author        = author,
        committer     = committer,
        parents       = parents
      )
    }

    "return an error if config cannot be read" in new TestCase {
      val exception = exceptions.generateOne
      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Error if remote client responds with invalid json" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      }.getMessage should startWith("Invalid message body")
    }

    "return an Error if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(notFound().withBody("some message"))
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.NotFound}; body: some message"
    }
  }

  private trait TestCase {
    val gitLabUrl     = url(externalServiceBaseUrl)
    val projectId     = projectIds.generateOne
    val commitId      = commitIds.generateOne
    val commitMessage = commitMessages.generateOne
    val committedDate = CommittedDate(LocalDateTime.of(2012, 9, 20, 9, 6, 12).atOffset(ZoneOffset.ofHours(3)).toInstant)
    val author        = users.generateOne
    val committer     = users.generateOne
    val parents       = parentsIdsLists().generateOne

    lazy val Right(responseJson) = parse {
      s"""
         |{
         |  "id": "$commitId",
         |  "author_name": "${author.username}",
         |  "author_email": "${author.email}",
         |  "committer_name": "${committer.username}",
         |  "committer_email": "${committer.email}",
         |  "message": "$commitMessage",
         |  "committed_date": "2012-09-20T09:06:12+03:00",
         |  "parent_ids": ${parents.map(parent => s""""$parent"""").mkString("[", ",", "]")}
         |}
      """.stripMargin
    }

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider.get _)
        .expects()
        .returning(returning)

    val finder = new IOCommitInfoFinder(configProvider)

    private def url(value: String) =
      RefType
        .applyRef[String Refined Url](value)
        .getOrElse(throw new IllegalArgumentException("Invalid url value"))
  }
}
