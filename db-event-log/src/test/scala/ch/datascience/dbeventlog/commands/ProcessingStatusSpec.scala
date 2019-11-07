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

package ch.datascience.dbeventlog.commands

import cats.implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.math.BigDecimal.RoundingMode
import scala.util.{Failure, Success, Try}

class ProcessingStatusSpec extends WordSpec with ScalaCheckPropertyChecks {

  "from" should {

    "return ProcessingStatus with progress = 100% for done = total = 0" in {
      val Success(processingStatus) = ProcessingStatus.from[Try](0, 0)

      processingStatus.done.value     shouldBe 0
      processingStatus.total.value    shouldBe 0
      processingStatus.progress.value shouldBe 100d
    }

    "return ProcessingStatus with progress calculated as (done / total) * 100" in {
      forAll {
        for {
          total <- positiveInts(max = Integer.MAX_VALUE)
          done  <- positiveInts(max = total.value)
        } yield done.value -> total.value
      } {
        case (done, total) =>
          val Success(processingStatus) = ProcessingStatus.from[Try](done, total)

          processingStatus.done.value  shouldBe done
          processingStatus.total.value shouldBe total

          val expectedProgress = BigDecimal((done.toDouble / total) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
          processingStatus.progress.value shouldBe expectedProgress
      }
    }

    "fail if done negative" in {
      forAll(negativeInts(min = Integer.MIN_VALUE), nonNegativeInts(max = Integer.MAX_VALUE)) {
        case (done, total) =>
          val Failure(exception) = ProcessingStatus.from[Try](done, total.value)
          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe "ProcessingStatus's 'done' cannot be negative"
      }
    }

    "fail if total negative" in {
      forAll(nonNegativeInts(max = Integer.MAX_VALUE), negativeInts(min = Integer.MIN_VALUE)) {
        case (done, total) =>
          val Failure(exception) = ProcessingStatus.from[Try](done.value, total)
          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe "ProcessingStatus's 'total' cannot be negative"
      }
    }

    "fail if done > total" in {
      forAll {
        for {
          done  <- positiveInts(max = Integer.MAX_VALUE)
          total <- positiveInts(max = done.value - 1)
        } yield done.value -> total.value
      } {
        case (done, total) =>
          val Failure(exception) = ProcessingStatus.from[Try](done, total)
          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe "ProcessingStatus with 'done' > 'total' makes no sense"
      }
    }
  }
}
