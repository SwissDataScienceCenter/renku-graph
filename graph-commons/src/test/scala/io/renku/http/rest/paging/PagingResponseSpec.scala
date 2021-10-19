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

package io.renku.http.rest.paging

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.rest.paging.model.{PerPage, Total}
import io.renku.testtools.IOSpec
import io.renku.tinytypes.TestTinyTypes.UrlTestType
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Failure, Success, Try}

class PagingResponseSpec extends AnyWordSpec with IOSpec with ScalaCheckPropertyChecks with should.Matchers {

  "from" should {

    "instantiate successfully when the number of results is less than the perPage and the requested page plus results equals total " +
      "-> (page - 1) * perPage + results.size == total" in {
        forAll(perPages, pages) { (perPage, page) =>
          forAll(nonEmptyList(nonBlankStrings(), maxElements = Refined.unsafeApply(perPage.value))) { results =>
            val total   = Total((page.value - 1) * perPage.value + results.size)
            val request = PagingRequest(page, perPage)

            val Success(response) = PagingResponse.from[Try, NonBlank](results.toList, request, total)

            response.results                  shouldBe results.toList
            response.pagingInfo.pagingRequest shouldBe request
            response.pagingInfo.total         shouldBe total
          }
        }
      }

    "instantiate successfully when the number of results equals the perPage and the requested page is within the total " +
      "-> results.size == perPage and (page - 1) * perPage + results.size < total" in {
        forAll(perPages, pages) { (perPage, page) =>
          val perPageValue: Int Refined Positive = Refined.unsafeApply(perPage.value)
          forAll(nonEmptyList(nonBlankStrings(), minElements = perPageValue, maxElements = perPageValue)) { results =>
            results should have size perPage.value

            val total = Gen
              .choose(
                min = (page.value - 1) * perPage.value + results.size + 1,
                max = Int.MaxValue
              )
              .map(Total(_))
              .generateOne
            val request = PagingRequest(page, perPage)

            val Success(response) = PagingResponse.from[Try, NonBlank](results.toList, request, total)

            response.results                  shouldBe results.toList
            response.pagingInfo.pagingRequest shouldBe request
            response.pagingInfo.total         shouldBe total
          }
        }
      }

    "instantiate successfully when there are no results and the requested page is not within the total " +
      "-> (page - 1) * perPage >= total" in {
        forAll(pages, perPages) { (page, perPage) =>
          forAll(Gen.choose(0, (page.value - 1) * perPage.value).map(Total(_))) { total =>
            val results = List.empty[NonBlank]

            val request = PagingRequest(page, perPage)

            val Success(response) = PagingResponse.from[Try, NonBlank](results, request, total)

            response.results                  shouldBe results
            response.pagingInfo.pagingRequest shouldBe request
            response.pagingInfo.total         shouldBe total
          }
        }
      }

    "fail when the number of results is less then the perPage and the requested page is within the total " +
      "-> results.size < perPage and (page - 1) * perPage + results.size < total" in {
        forAll(perPages.retryUntil(_.value > 1), pages) { (perPage, page) =>
          forAll(nonEmptyList(nonBlankStrings(), maxElements = Refined.unsafeApply(perPage.value - 1))) { results =>
            val validTotalValue = (page.value - 1) * perPage.value + results.size
            val total           = Gen.choose(validTotalValue + 1, Int.MaxValue).map(Total(_)).generateOne
            val request         = PagingRequest(page, PerPage(perPage.value))

            val Failure(exception) = PagingResponse.from[Try, NonBlank](results.toList, request, total)

            exception shouldBe an[IllegalArgumentException]
            exception.getMessage shouldBe s"PagingResponse cannot be instantiated for ${results.size} results, total: $total, page: $page and perPage: ${perPage.value}"
          }
        }
      }

    "fail when there are no results and the requested page is within the total " +
      "-> (page - 1) * perPage < total" in {
        forAll(pages, perPages) { (page, perPage) =>
          forAll(Gen.choose((page.value - 1) * perPage.value + 1, Int.MaxValue).map(Total(_))) { total =>
            val results = List.empty[NonBlank]
            val request = PagingRequest(page, perPage)

            val Failure(exception) = PagingResponse.from[Try, NonBlank](results, request, total)

            exception shouldBe an[IllegalArgumentException]
            exception.getMessage shouldBe s"PagingResponse cannot be instantiated for ${results.size} results, total: $total, page: $page and perPage: $perPage"
          }
        }
      }
  }

  "updateResults" should {

    "successfully replace the results with the given results if they have the same number of elements" in {

      val response = pagingResponses(nonBlankStrings()).generateOne

      val newResults = Gen.listOfN(response.results.size, nonBlankStrings()).generateOne

      val Success(updated) = response.updateResults[Try](newResults)

      updated.results    shouldBe newResults
      updated.pagingInfo shouldBe response.pagingInfo
    }

    "fail replacing the results if the new results have different number of elements" in {

      val response = pagingResponses(nonBlankStrings()).generateOne

      val newResultsNumber = nonNegativeInts() generateDifferentThan Refined.unsafeApply(response.results.size)
      val newResults       = Gen.listOfN(newResultsNumber.value, nonBlankStrings()).generateOne

      val Failure(exception) = response.updateResults[Try](newResults)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "Cannot update Paging Results as there's different number of results"
    }
  }

  "toHttpResponse" should {

    import cats.effect.IO
    import io.circe.syntax._
    import io.renku.http.server.EndpointTester._
    import org.http4s.MediaType.application
    import org.http4s.Status._
    import org.http4s.headers.`Content-Type`

    "return Ok with response results in Json body and paging headers" in {

      implicit lazy val resourceUrl: UrlTestType = httpUrls().generateAs[UrlTestType]
      val response = pagingResponses(nonBlankStrings().map(_.value)).generateOne

      val httpResponse = response.toHttpResponse[IO, UrlTestType]

      httpResponse.status                         shouldBe Ok
      httpResponse.contentType                    shouldBe Some(`Content-Type`(application.json))
      httpResponse.headers.headers                  should contain allElementsOf PagingHeaders.from(response)
      httpResponse.as[List[Json]].unsafeRunSync() shouldBe response.results.map(_.asJson)
    }
  }
}
