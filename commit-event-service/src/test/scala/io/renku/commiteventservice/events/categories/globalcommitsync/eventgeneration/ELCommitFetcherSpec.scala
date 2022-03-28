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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.Encoder._
import io.circe.literal._
import io.circe.syntax._
import io.renku.commiteventservice.events.categories.globalcommitsync.Generators.{dateConditions, untilDateConditions}
import io.renku.generators.CommonGraphGenerators.{pages, pagingRequests}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.CommitId
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ELCommitFetcherSpec extends AnyWordSpec with IOSpec with ExternalServiceStubbing with should.Matchers {

  "fetchGitLabCommits" should {

    "fetch commits from the given page" in new TestCase {

      val condition = dateConditions.generateOne
      val conditionQuery = {
        val (name, value) = condition.asQueryParameter
        s"$name=${urlEncode(value)}"
      }

      val maybeNextPage = pages.generateOption
      stubFor {
        get(
          s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&$conditionQuery"
        ).willReturn(
          okJson(commitIdsList.asJson.noSpaces)
            .withHeader("Next-Page", maybeNextPage.map(_.show).getOrElse(""))
        )
      }

      elCommitFetcher
        .fetchELCommits(projectPath, condition, pageRequest)
        .unsafeRunSync() shouldBe PageResult(commitIdsList, maybeNextPage)
    }

    "return no commits if there aren't any" in new TestCase {

      val dateCondition = untilDateConditions.generateOne

      stubFor {
        get(
          s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&until=${urlEncode(dateCondition.date.toString)}"
        ).willReturn(okJson("[]"))
      }

      elCommitFetcher
        .fetchELCommits(projectPath, dateCondition, pageRequest)
        .unsafeRunSync() shouldBe PageResult.empty
    }

    "return an empty list if project for NOT_FOUND" in new TestCase {

      val dateCondition = untilDateConditions.generateOne

      stubFor {
        get(
          s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&until=${urlEncode(dateCondition.date.toString)}"
        ).willReturn(notFound())
      }

      elCommitFetcher
        .fetchELCommits(projectPath, dateCondition, pageRequest)
        .unsafeRunSync() shouldBe PageResult.empty
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      val dateCondition = untilDateConditions.generateOne

      stubFor {
        get(
          s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&until=${urlEncode(dateCondition.date.toString)}"
        ).willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        elCommitFetcher.fetchELCommits(projectPath, dateCondition, pageRequest).unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&until=${urlEncode(
        dateCondition.date.toString
      )} returned ${Status.BadRequest}; body: some error"
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      val dateCondition = untilDateConditions.generateOne

      stubFor {
        get(
          s"/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&until=${urlEncode(dateCondition.date.toString)}"
        ).willReturn(okJson("{}"))
      }

      intercept[Exception] {
        elCommitFetcher.fetchELCommits(projectPath, dateCondition, pageRequest).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $eventLogUrl/events?project-path=$projectPath&page=${pageRequest.page}&per_page=${pageRequest.perPage}&until=${urlEncode(
          dateCondition.date.toString
        )} returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private trait TestCase {
    val projectPath   = projectPaths.generateOne
    val pageRequest   = pagingRequests.generateOne
    val commitIdsList = commitIds.generateNonEmptyList().toList

    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    private implicit val logger: TestLogger[IO] = TestLogger()
    val elCommitFetcher = new ELCommitFetcherImpl[IO](eventLogUrl)
  }

  private implicit lazy val encoder: Encoder[CommitId] = Encoder.instance { commitId =>
    json"""{
      "id":              ${commitId.value},
      "status":          ${eventStatuses.generateOne.value},
      "processingTimes": [],
      "date":            ${timestampsNotInTheFuture.generateOne},
      "executionDate":   ${timestampsNotInTheFuture.generateOne}
    }"""
  }
}
