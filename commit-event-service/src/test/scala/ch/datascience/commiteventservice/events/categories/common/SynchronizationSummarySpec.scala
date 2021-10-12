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

package ch.datascience.commiteventservice.events.categories.common

import ch.datascience.commiteventservice.events.categories.common.UpdateResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SynchronizationSummarySpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "get & updated" should {

    "return count for the given key" in {
      forAll(resultsAndMaybeCountList) { countsAndResults =>
        val summary = countsAndResults
          .foldLeft(SynchronizationSummary()) {
            case (summary, (Some(count), result)) => summary.updated(result, count)
            case (summary, (None, _))             => summary
          }

        countsAndResults.foreach {
          case (Some(count), result) => summary.get(result) shouldBe count
          case (None, result)        => summary.get(result) shouldBe 0
        }
      }
    }
  }

  "increment" should {

    "increment the count for the given key" in {
      forAll(summaries, updateResults.toGeneratorOfList(maxElements = 10)) { (initialSummary, results) =>
        val summary = results.foldLeft(initialSummary)(_.incrementCount(_))

        summary.get(Skipped) shouldBe initialSummary.get(Skipped) + results.count(_ == Skipped)
        summary.get(Created) shouldBe initialSummary.get(Created) + results.count(_ == Created)
        summary.get(Existed) shouldBe initialSummary.get(Existed) + results.count(_ == Existed)
        summary.get(Deleted) shouldBe initialSummary.get(Deleted) + results.count(_ == Deleted)
        val failed = Failed(nonEmptyStrings().generateOne, exceptions.generateOne)
        summary.get(failed) shouldBe initialSummary.get(failed) + results.count(_.isInstanceOf[Failed])
      }
    }
  }

  private lazy val updateResults: Gen[UpdateResult] =
    Gen.oneOf(Skipped, Created, Existed, Deleted, Failed(nonEmptyStrings().generateOne, exceptions.generateOne))

  private lazy val resultsAndMaybeCountList: Gen[List[(Option[Int], UpdateResult)]] =
    nonNegativeInts().map(_.value).toGeneratorOfOptions.toGeneratorOfList(minElements = 5, maxElements = 5).map {
      maybeCounts =>
        maybeCounts
          .zip(Set(Skipped, Created, Existed, Deleted, Failed(nonEmptyStrings().generateOne, exceptions.generateOne)))
    }

  private lazy val summaries: Gen[SynchronizationSummary] =
    resultsAndMaybeCountList.map {
      _.foldLeft(SynchronizationSummary()) {
        case (summary, (Some(count), result)) => summary.updated(result, count)
        case (summary, (None, _))             => summary
      }
    }
}
