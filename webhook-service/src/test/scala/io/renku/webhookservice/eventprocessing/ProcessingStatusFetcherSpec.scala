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

package io.renku.webhookservice.eventprocessing

import Generators._
import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProcessingStatusFetcherSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers with IOSpec {

  "fetchProcessingStatus" should {

    "return ProcessingStatus if returned from the Event Log" in new TestCase {

      val processingStatus = nonZeroProgressStatuses.generateOne

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(okJson(processingStatus.asJson.spaces2))
      }

      fetcher.fetchProcessingStatus(projectId).unsafeRunSync() shouldBe processingStatus
    }

    "return None if NOT_FOUND returned the Event Log" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(notFound())
      }

      fetcher.fetchProcessingStatus(projectId).unsafeRunSync() shouldBe ProgressStatus.Zero
    }

    "return a RuntimeException if remote client responds with status different than OK and NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        fetcher.fetchProcessingStatus(projectId).unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/processing-status?project-id=$projectId returned ${Status.BadRequest}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(okJson(json"""{}""".spaces2))
      }

      intercept[Exception] {
        fetcher.fetchProcessingStatus(projectId).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $eventLogUrl/processing-status?project-id=$projectId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }

    "return a RuntimeException if remote client responds with invalid body" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(okJson(json"""{"done": "1", "total": 2, "progress": 3}""".spaces2))
      }

      intercept[Exception] {
        fetcher.fetchProcessingStatus(projectId).unsafeRunSync()
      }.getMessage shouldBe s"""GET $eventLogUrl/processing-status?project-id=$projectId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {"done" : "1","total" : 2,"progress" : 3}; ProcessingStatus's 'progress' is invalid"""
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectId   = projectIds.generateOne
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val fetcher     = new ProcessingStatusFetcherImpl[IO](eventLogUrl)
  }
}
