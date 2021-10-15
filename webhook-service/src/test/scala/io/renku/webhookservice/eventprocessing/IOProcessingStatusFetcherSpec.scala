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

package io.renku.webhookservice.eventprocessing

import ProcessingStatusGenerator._
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.Refined
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.math.BigDecimal.RoundingMode

class IOProcessingStatusFetcherSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "fetchProcessingStatus" should {

    "return some ProcessingStatus if returned from the Event Log" in new TestCase {

      val processingStatus = processingStatuses.generateOne

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(okJson(processingStatus.asJson.spaces2))
      }

      fetcher.fetchProcessingStatus(projectId).value.unsafeRunSync() shouldBe processingStatus.some
    }

    "return None if NOT_FOUND returned the Event Log" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(notFound())
      }

      fetcher.fetchProcessingStatus(projectId).value.unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote client responds with status different than OK and NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        fetcher.fetchProcessingStatus(projectId).value.unsafeRunSync()
      }.getMessage shouldBe s"GET $eventLogUrl/processing-status?project-id=$projectId returned ${Status.BadRequest}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/processing-status?project-id=$projectId")
          .willReturn(okJson(json"""{}""".spaces2))
      }

      intercept[Exception] {
        fetcher.fetchProcessingStatus(projectId).value.unsafeRunSync()
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
        fetcher.fetchProcessingStatus(projectId).value.unsafeRunSync()
      }.getMessage shouldBe s"""GET $eventLogUrl/processing-status?project-id=$projectId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {"done" : "1","total" : 2,"progress" : 3}; ProcessingStatus's 'progress' is invalid"""
    }
  }

  "ProcessingStatus.from" should {

    import eu.timepit.refined.auto._
    import io.renku.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus._

    "succeed if done is 0" in {
      val done:     Done     = 0
      val total:    Total    = positiveInts().generateOne
      val progress: Progress = 0d
      val Right(status) = ProcessingStatus.from(done.value, total.value, progress.value)

      status.done     shouldBe done
      status.total    shouldBe total
      status.progress shouldBe progress
    }

    "instantiate ProcessingStatus if done, total and progress are valid" in {
      forAll(nonNegativeInts(), nonNegativeInts()) { (done, totalDiff) =>
        val total: Total = Refined.unsafeApply(done.value + totalDiff.value)
        val progress: Progress = Refined.unsafeApply(
          BigDecimal((done.value.toDouble / total.value) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
        )

        val Right(status) = ProcessingStatus.from(done.value, total.value, progress.value)

        status.done     shouldBe done
        status.total    shouldBe total
        status.progress shouldBe progress
      }
    }

    "fail if done < 0" in {
      forAll(negativeInts(), positiveInts()) { (done, total) =>
        val progress = BigDecimal((done.toDouble / total.value) * 100)
          .setScale(2, RoundingMode.HALF_DOWN)
          .toDouble

        ProcessingStatus.from(done, total.value, progress) shouldBe Left(
          "ProcessingStatus's 'done' cannot be negative"
        )
      }
    }

    "fail if total < 0" in {
      forAll(nonNegativeInts(), negativeInts()) { (done, total) =>
        ProcessingStatus.from(done.value, total, 0d) shouldBe Left(
          "ProcessingStatus's 'total' cannot be negative"
        )
      }
    }

    "fail if total < done" in {
      val done  = positiveInts().generateOne.value + 1
      val total = done - 1
      val progress = BigDecimal((done.toDouble / total) * 100)
        .setScale(2, RoundingMode.HALF_DOWN)
        .toDouble

      ProcessingStatus.from(done, total, progress) shouldBe Left(
        "ProcessingStatus's 'done' > 'total'"
      )
    }

    "fail if progress is invalid" in {
      val done     = nonNegativeInts().generateOne.value + 1
      val total    = done + positiveInts().generateOne.value
      val progress = 2d

      ProcessingStatus.from(done, total, progress) shouldBe Left(
        "ProcessingStatus's 'progress' is invalid"
      )
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val eventLogUrl = EventLogUrl(externalServiceBaseUrl)
    val fetcher     = new ProcessingStatusFetcherImpl[IO](eventLogUrl, TestLogger())
  }

  private implicit val processingStatusEncoder: Encoder[ProcessingStatus] = Encoder.instance[ProcessingStatus] {
    case ProcessingStatus(done, total, progress) => json"""{
      "done":     ${done.value},
      "total":    ${total.value},
      "progress": ${progress.value}
    }"""
  }
}
