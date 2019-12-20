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

package ch.datascience.http.rest.paging

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.model.{Page, PerPage, Total}
import eu.timepit.refined.auto._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class PagingSpec extends WordSpec {

  "findPage" should {

    "call the findResults and return found items " +
      "if the first page requested and per page is greater than the number of found items" in {

      val results = nonEmptyList(positiveInts(), minElements = 6, maxElements = 6).generateOne.map(_.value).toList

      val resultsFinder = new ResultsFinder(returning = context.pure(results))()

      val pagingRequest = PagingRequest(Page.first, PerPage(7))

      val Success(response) = resultsFinder.find(pagingRequest)

      response.results                  shouldBe results
      response.pagingInfo.pagingRequest shouldBe pagingRequest
      response.pagingInfo.total         shouldBe Total(6)
    }

    "call the findResults and find total number of results " +
      "if the combination of requested page and per page is equal to the number of found items" in {

      val results = nonEmptyList(positiveInts(), minElements = 6, maxElements = 6).generateOne.map(_.value).toList

      val resultsFinder = new ResultsFinder(returning = context.pure(results))()

      val pagingRequest = PagingRequest(Page.first, PerPage(6))

      val Success(response) = resultsFinder.find(pagingRequest)

      response.results                  shouldBe results
      response.pagingInfo.pagingRequest shouldBe pagingRequest
      response.pagingInfo.total         shouldBe Total(results.size)
    }

    "call the findResults and find total number of results " +
      "if not the first page is requested and there are no results" in {

      val total         = Total(5)
      val resultsFinder = new ResultsFinder(returning = context.pure(Nil))(total)

      val pagingRequest = PagingRequest(Page(2), PerPage(6))

      val Success(response) = resultsFinder.find(pagingRequest)

      response.results                  shouldBe Nil
      response.pagingInfo.pagingRequest shouldBe pagingRequest
      response.pagingInfo.total         shouldBe total
    }

    "call the findResults and find total number of results " +
      "if the combination of requested page and per page is less than the number of found items" in {

      val results = nonEmptyList(positiveInts(), minElements = 6, maxElements = 6).generateOne.map(_.value).toList

      val resultsFinder = new ResultsFinder(returning = context.pure(results))()

      val pagingRequest = PagingRequest(Page.first, PerPage(5))

      val Success(response) = resultsFinder.find(pagingRequest)

      response.results                  shouldBe results.take(PerPage(5).value)
      response.pagingInfo.pagingRequest shouldBe pagingRequest
      response.pagingInfo.total         shouldBe Total(results.size)
    }
  }

  private val context = MonadError[Try, Throwable]

  private class ResultsFinder(
      returning: Try[List[Int]]
  )(total:       Total = returning.map(_.size).map(Total(_)).getOrElse(Total(0)))
      extends Paging[Try, Int] {

    private implicit val resultsFinder: PagedResultsFinder[Try, Int] = new PagedResultsFinder[Try, Int] {

      override def findResults(paging: PagingRequest): Try[List[Int]] = {
        val startIdx = (paging.page.value - 1) * paging.perPage.value
        returning.map(_.slice(startIdx, startIdx + paging.perPage.value))
      }

      override def findTotal(): Try[Total] = total.pure[Try]
    }

    def find(paging: PagingRequest): Try[PagingResponse[Int]] = findPage(paging)
  }
}
