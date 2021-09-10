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

package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.{ConcurrentEffect, IO, Timer}
import ch.datascience.commiteventservice.events.categories.common.CommitInfo
import ch.datascience.commiteventservice.events.categories.common.Generators.commitInfos
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GitLabUrl
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.CommitId
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class LatestCommitFinderSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "findLatestCommitId" should {
    "return the latest CommitID if remote responds with OK and valid body - personal access token case" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(urlPathEqualTo(s"/api/v4/projects/$projectId/repository/commits"))
          .withQueryParam("per_page", equalTo("1"))
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(commitsJson(from = commitId)))
      }

      latestCommitFinder.findLatestCommitId(projectId, Some(personalAccessToken)).value.unsafeRunSync() shouldBe Some(
        commitId
      )
    }
  }

  private implicit val ce:    ConcurrentEffect[IO] = IO.ioConcurrentEffect(IO.contextShift(global))
  private implicit val timer: Timer[IO]            = IO.timer(global)

  private trait TestCase {
    val gitLabUrl = GitLabUrl(externalServiceBaseUrl)
    val projectId = projectIds.generateOne
    val commitId  = commitIds.generateOne

    val latestCommitFinder = new LatestCommitFinderImpl(gitLabUrl, Throttler.noThrottling, TestLogger())
  }

  private def commitsJson(from: CommitId) =
    Json.arr(commitJson(commitInfos.generateOne.copy(id = from))).noSpaces

  private def commitJson(commitInfo: CommitInfo) = json"""{
    "id":              ${commitInfo.id.value},
    "author_name":     ${commitInfo.author.name.value},
    "author_email":    ${commitInfo.author.emailToJson},
    "committer_name":  ${commitInfo.committer.name.value},
    "committer_email": ${commitInfo.committer.emailToJson},
    "message":         ${commitInfo.message.value},
    "committed_date":  ${commitInfo.committedDate.value},
    "parent_ids":      ${commitInfo.parents.map(_.value)}
  }"""
}
