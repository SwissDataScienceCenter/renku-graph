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

import ProgressStatus._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.webhookservice.eventprocessing.Generators.nonZeroProgressStatuses
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.math.BigDecimal.RoundingMode

class ProgressStatusSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "succeed if done is 0" in {
      val done:     Done     = 0
      val total:    Total    = positiveInts().generateOne
      val progress: Progress = 0d
      val Right(status) = ProgressStatus.from(done.value, total.value, progress.value)

      status.done     shouldBe done
      status.total    shouldBe total
      status.progress shouldBe progress
    }

    "instantiate ProgressStatus if done, total and progress are valid" in {
      forAll(nonNegativeInts(), nonNegativeInts()) { (done, totalDiff) =>
        val total: Total = Refined.unsafeApply(done.value + totalDiff.value)
        val progress: Progress = Refined.unsafeApply(
          BigDecimal((done.value.toDouble / total.value) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
        )

        val Right(status) = ProgressStatus.from(done.value, total.value, progress.value)

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

        ProgressStatus.from(done, total.value, progress) shouldBe Left(
          "ProcessingStatus's 'done' cannot be negative"
        )
      }
    }

    "fail if total < 0" in {
      forAll(nonNegativeInts(), negativeInts()) { (done, total) =>
        ProgressStatus.from(done.value, total, 0d) shouldBe Left(
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

      ProgressStatus.from(done, total, progress) shouldBe Left(
        "ProcessingStatus's 'done' > 'total'"
      )
    }

    "fail if progress is invalid" in {
      val done     = nonNegativeInts().generateOne.value + 1
      val total    = done + positiveInts().generateOne.value
      val progress = 2d

      ProgressStatus.from(done, total, progress) shouldBe Left(
        "ProcessingStatus's 'progress' is invalid"
      )
    }
  }

  "encode" should {

    "encode a NonZero ProgressStatus to the following json object" in {
      val progress = nonZeroProgressStatuses.generateOne

      progress.asJson shouldBe json"""{
        "done":     ${progress.done.value},
        "total":    ${progress.total.value},
        "progress": ${progress.progress.value}
      }"""
    }

    "encode a Zero ProgressStatus to the following json object" in {
      ProgressStatus.Zero.asJson shouldBe json"""{
        "done":  0,
        "total": 0
      }"""
    }
  }
}
