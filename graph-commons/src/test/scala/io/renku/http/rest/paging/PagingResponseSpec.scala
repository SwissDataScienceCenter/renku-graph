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

package io.renku.http.rest.paging

import eu.timepit.refined.api.Refined
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.rest.paging.model.{Page, PerPage, Total}
import io.renku.testtools.IOSpec
import io.renku.tinytypes.TestTinyTypes.UrlTestType
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Failure, Success, Try}

class PagingResponseSpec extends AnyWordSpec with IOSpec with ScalaCheckPropertyChecks with should.Matchers {

  "from" should {

    "fail if the number of results > perPage" in {
      forAll(perPages, pages) { (perPage, page) =>
        val results = nonEmptyStrings().generateList(min = perPage.value + 1, max = perPage.value * 2)
        val total   = Total((page.value - 1) * perPage.value + results.size)
        val request = PagingRequest(page, perPage)

        val Failure(exception) = PagingResponse.from[Try, String](results, request, total)

        exception shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"PagingResponse cannot be instantiated for ${results.size} results, total: $total, page: $page and perPage: $perPage"
      }
    }

    "fix the total if results is not empty and (page.value - 1) * perPage.value + results.size > total.value" in {
      forAll(perPages.retryUntil(_.value > 1), pages.retryUntil(_.value > 1)) { (perPage, page) =>
        val results = nonEmptyStrings().generateNonEmptyList(max = perPage.value).toList
        val total   = positiveInts((page.value - 1) * perPage.value + results.size - 1).map(_.value).generateAs(Total)
        val request = PagingRequest(page, perPage)

        val Success(response) = PagingResponse.from[Try, String](results, request, total)

        response.results                  shouldBe results
        response.pagingInfo.pagingRequest shouldBe request
        response.pagingInfo.total         shouldBe Total((page.value - 1) * perPage.value + results.size)
      }
    }

    "instantiate successfully if results list is empty and (page.value - 1) * perPage.value > total.value" in {
      forAll(perPages.retryUntil(_.value > 1), pages.retryUntil(_.value > 1)) { (perPage, page) =>
        val results = List.empty[String]
        val total   = positiveInts((page.value - 1) * perPage.value + results.size - 1).map(_.value).generateAs(Total)
        val request = PagingRequest(page, perPage)

        val Success(response) = PagingResponse.from[Try, String](results, request, total)

        response.results                  shouldBe results
        response.pagingInfo.pagingRequest shouldBe request
        response.pagingInfo.total         shouldBe total
      }
    }

    "instantiate successfully in other cases" in {
      forAll(perPages, pages) { (perPage, page) =>
        val results = nonEmptyStrings().generateNonEmptyList(max = perPage.value).toList
        val total   = ints(min = (page.value - 1) * perPage.value + results.size).generateAs(Total)
        val request = PagingRequest(page, perPage)

        val Success(response) = PagingResponse.from[Try, String](results, request, total)

        response.results                  shouldBe results
        response.pagingInfo.pagingRequest shouldBe request
        response.pagingInfo.total         shouldBe total
      }
    }
  }

  "from with no total given" should {

    "calculate the total from the given results if results size <= perPage - case of the first page" in {
      val paging  = pagingRequests.generateOne.copy(page = Page(1))
      val results = nonBlankStrings().generateNonEmptyList(max = paging.perPage.value).toList

      val Success(actual) = PagingResponse.from[Try, NonBlank](results, paging)

      actual.results                  shouldBe results
      actual.pagingInfo.pagingRequest shouldBe paging
      actual.pagingInfo.total         shouldBe Total(results.size)
    }

    "calculate the total from the given results if results size <= perPage - case not of the first page" in {
      val paging = pagingRequests.generateOne.copy(page = Page(2))
      val results = nonBlankStrings()
        .generateNonEmptyList(min = paging.perPage.value + 1, max = paging.page.value * paging.perPage.value)
        .toList

      val Success(actual) = PagingResponse.from[Try, NonBlank](results, paging)

      actual.results                  shouldBe results.drop(paging.perPage.value)
      actual.pagingInfo.pagingRequest shouldBe paging
      actual.pagingInfo.total         shouldBe Total(results.size)
    }

    "accept an empty results list if page 1 requested" in {
      val paging = pagingRequests.generateOne.copy(page = Page(1))

      val Success(actual) = PagingResponse.from[Try, NonBlank](List.empty, paging)

      actual.results                  shouldBe Nil
      actual.pagingInfo.pagingRequest shouldBe paging
      actual.pagingInfo.total         shouldBe Total(0)
    }

    "fail if requested page and perPage is beyond the number of results" in {
      val paging =
        PagingRequest(page = Gen.oneOf(2, 3, 4, 5).generateAs(Page), perPage = ints(2, 100).generateAs[PerPage])
      val results = nonBlankStrings()
        .generateNonEmptyList(max = paging.perPage.value - 1)
        .toList

      val Failure(exception) = PagingResponse.from[Try, NonBlank](results, paging)

      exception shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"PagingResponse cannot be instantiated for ${results.size} results, total: ${results.size}, page: ${paging.page} and perPage: ${paging.perPage}"
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
