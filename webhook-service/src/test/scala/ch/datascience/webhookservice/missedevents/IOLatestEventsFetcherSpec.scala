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

package ch.datascience.webhookservice.missedevents

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.missedevents.LatestEventsFetcher._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOLatestEventsFetcherSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "fetchLatestEvents" should {

    "return a list of latest projects commits fetched from the Event Log" in new TestCase {

      val latestProjectCommitsList = latestProjectsCommits.generateNonEmptyList().toList

      stubFor {
        get("/events?latest-per-project=true")
          .willReturn(okJson(latestProjectCommitsList.asJson.spaces2))
      }

      fetcher.fetchLatestEvents().unsafeRunSync() shouldBe latestProjectCommitsList
    }

    "return an empty list if nothing gets fetched from the Event Log" in new TestCase {

      stubFor {
        get("/events?latest-per-project=true")
          .willReturn(okJson(Json.arr().spaces2))
      }

      fetcher.fetchLatestEvents().unsafeRunSync() shouldBe Nil
    }

    "return a RuntimeException if remote client responds with status different than OK" in new TestCase {

      stubFor {
        get("/events?latest-per-project=true")
          .willReturn(notFound().withBody("some error"))
      }

      intercept[Exception] {
        fetcher.fetchLatestEvents().unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/events?latest-per-project=true returned ${Status.NotFound}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get("/events?latest-per-project=true")
          .willReturn(okJson(json"""{}""".spaces2))
      }

      intercept[Exception] {
        fetcher.fetchLatestEvents().unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/events?latest-per-project=true returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val fetcher     = new IOLatestEventsFetcher(eventLogUrl, TestLogger())
  }

  private implicit val latestCommitsEncoder: Encoder[LatestProjectCommit] = Encoder.instance[LatestProjectCommit] {
    case LatestProjectCommit(commitId, Project(projectId, projectPath)) => json"""{
      "project": {
        "id": ${projectId.value},
        "path": ${projectPath.value}
      },
      "body": ${json"""{"id": ${commitId.value}}""".noSpaces}
    }"""
  }
}
