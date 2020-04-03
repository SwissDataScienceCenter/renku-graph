/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.missedevents.LatestEventsFinder.LatestProjectCommit
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.http4s.Status
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOLatestEventsFinderSpec extends WordSpec with ExternalServiceStubbing {

  "fetchLatestEvents" should {

    "return a list of latest projects commits fetched from the Event Log" in new TestCase {

      val latestProjectCommitsList = latestProjectCommits.generateNonEmptyList().toList

      stubFor {
        get("/events/latest")
          .willReturn(okJson(latestProjectCommitsList.asJson.spaces2))
      }

      finder.fetchLatestEvents.unsafeRunSync() shouldBe latestProjectCommitsList
    }

    "return an empty list if nothing gets fetched from the Event Log" in new TestCase {

      stubFor {
        get("/events/latest")
          .willReturn(okJson(Json.arr().spaces2))
      }

      finder.fetchLatestEvents.unsafeRunSync() shouldBe Nil
    }

    "return a RuntimeException if remote client responds with status different than OK" in new TestCase {

      stubFor {
        get("/events/latest")
          .willReturn(notFound().withBody("some error"))
      }

      intercept[Exception] {
        finder.fetchLatestEvents.unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/events/latest returned ${Status.NotFound}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get("/events/latest")
          .willReturn(okJson(json"""{}""".spaces2))
      }

      intercept[Exception] {
        finder.fetchLatestEvents.unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/events/latest returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val finder      = new IOLatestEventsFinder(eventLogUrl, TestLogger())
  }

  private implicit val latestProjectCommits: Gen[LatestProjectCommit] = for {
    projectId <- projectIds
    commitId  <- commitIds
  } yield LatestProjectCommit(commitId, projectId)

  private implicit val latestCommitsEncoder: Encoder[LatestProjectCommit] = Encoder.instance[LatestProjectCommit] {
    case LatestProjectCommit(commitId, projectId) => json"""{
      "project": {
        "id": ${projectId.value}
      },
      "body": ${json"""{"id": ${commitId.value}}""".noSpaces}
    }"""
  }
}
