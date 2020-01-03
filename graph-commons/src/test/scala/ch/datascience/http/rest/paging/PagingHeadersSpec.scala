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

package ch.datascience.http.rest.paging

import PagingRequest.Decoders.page.{parameterName => pageParamName}
import PagingRequest.Decoders.perPage.{parameterName => perPageParamName}
import cats.implicits._
import ch.datascience.config.renku
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.rest.paging.PagingResponse.PagingInfo
import ch.datascience.http.rest.paging.model.Page.first
import eu.timepit.refined.api.Refined
import org.http4s._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class PagingHeadersSpec extends WordSpec with ScalaCheckPropertyChecks {

  import PagingHeaders._

  "from" should {

    s"generate $Total, $TotalPages, $PerPage, $Page, $NextPage, $PrevPage and $Link headers " +
      "if current page is neither the first nor the last page" in {

      forAll(currentPageNeitherFirstNorLast) { response =>
        import response._
        import response.pagingInfo._
        import response.pagingInfo.pagingRequest._

        implicit val resourceUrl: renku.ResourceUrl = renkuResourceUrlFrom(page, perPage)

        val totalPages = findTotalPages(pagingInfo)
        PagingHeaders.from(response) should contain theSameElementsAs Set(
          Header("Total", total.toString),
          Header("Total-Pages", totalPages.toString),
          Header("Per-Page", perPage.toString),
          Header("Page", page.toString),
          Header("Next-Page", (page.value + 1).toString),
          Header("Prev-Page", (page.value - 1).toString),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> (page.value + 1))}>; rel="next""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> (page.value - 1))}>; rel="prev""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> first.value)}>; rel="first""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> totalPages)}>; rel="last"""")
        )
      }
    }

    s"generate $Total, $TotalPages, $PerPage, $Page, $PrevPage and $Link headers " +
      "if current page is the last page" in {

      forAll(currentPageLast) { response =>
        import response._
        import response.pagingInfo._
        import response.pagingInfo.pagingRequest._

        implicit val resourceUrl: renku.ResourceUrl = renkuResourceUrlFrom(page, perPage)

        val totalPages = findTotalPages(pagingInfo)
        PagingHeaders.from(response) should contain theSameElementsAs Set(
          Header("Total", total.toString),
          Header("Total-Pages", totalPages.toString),
          Header("Per-Page", perPage.toString),
          Header("Page", page.toString),
          Header("Prev-Page", (page.value - 1).toString),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> (page.value - 1))}>; rel="prev""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> first.value)}>; rel="first""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> totalPages)}>; rel="last"""")
        )
      }
    }

    s"generate $Total, $TotalPages, $PerPage, $Page, $NextPage and $Link headers " +
      "if current page is the first page" in {

      forAll(currentPageFirst) { response =>
        import response._
        import response.pagingInfo._
        import response.pagingInfo.pagingRequest._

        implicit val resourceUrl: renku.ResourceUrl = renkuResourceUrlFrom(page, perPage)

        val totalPages = findTotalPages(pagingInfo)
        PagingHeaders.from(response) should contain theSameElementsAs Set(
          Header("Total", total.toString),
          Header("Total-Pages", totalPages.toString),
          Header("Per-Page", perPage.toString),
          Header("Page", page.toString),
          Header("Next-Page", (page.value + 1).toString),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> (page.value + 1))}>; rel="next""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> first.value)}>; rel="first""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> totalPages)}>; rel="last"""")
        )
      }
    }

    s"generate $Total, $TotalPages, $PerPage, $Page and $Link headers " +
      "if there's one page only" in {

      forAll(onePageOnly) { response =>
        import response.pagingInfo._
        import response.pagingInfo.pagingRequest._

        implicit val resourceUrl: renku.ResourceUrl = renkuResourceUrlFrom(page, perPage)

        PagingHeaders.from(response) should contain theSameElementsAs Set(
          Header("Total", total.toString),
          Header("Total-Pages", "1"),
          Header("Per-Page", perPage.toString),
          Header("Page", page.toString),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> first.value)}>; rel="first""""),
          Header("Link", s"""<${resourceUrl ? (pageParamName -> 1)}>; rel="last"""")
        )
      }
    }
  }

  private def renkuResourceUrlFrom(page: model.Page, perPage: model.PerPage): renku.ResourceUrl =
    renkuResourceUrls().generateOne ? (pageParamName -> page.toString) & (perPageParamName -> perPage.toString)

  private def findTotalPages(pagingInfo: PagingInfo): Int = {
    import pagingInfo._
    import pagingInfo.pagingRequest._

    if ((total.value / perPage.value.toFloat).isWhole()) total.value / perPage.value
    else total.value / perPage.value + 1
  }

  private lazy val currentPageNeitherFirstNorLast: Gen[PagingResponse[NonBlank]] =
    for {
      page    <- pages.retryUntil(_.value > 2)
      perPage <- perPages
      results <- nonEmptyList(nonBlankStrings(),
                              minElements = Refined.unsafeApply(perPage.value),
                              maxElements = Refined.unsafeApply(perPage.value))
      total = model.Total((page.value - 1) * perPage.value + results.size)
      currentPage <- Gen.choose(2, page.value - 1).map(model.Page(_))
    } yield PagingResponse
      .from[Try, NonBlank](results.toList, PagingRequest(currentPage, perPage), total)
      .fold(throw _, identity)

  private lazy val currentPageLast: Gen[PagingResponse[NonBlank]] =
    for {
      page    <- pages.retryUntil(_.value > 1)
      perPage <- perPages
      results <- nonEmptyList(nonBlankStrings(),
                              minElements = Refined.unsafeApply(perPage.value),
                              maxElements = Refined.unsafeApply(perPage.value))
      total = model.Total((page.value - 1) * perPage.value + results.size)
    } yield PagingResponse
      .from[Try, NonBlank](results.toList, PagingRequest(page, perPage), total)
      .fold(throw _, identity)

  private lazy val currentPageFirst: Gen[PagingResponse[NonBlank]] =
    for {
      page    <- pages.retryUntil(_.value > 1)
      perPage <- perPages
      results <- nonEmptyList(nonBlankStrings(),
                              minElements = Refined.unsafeApply(perPage.value),
                              maxElements = Refined.unsafeApply(perPage.value))
      total = model.Total((page.value - 1) * perPage.value + results.size)
    } yield PagingResponse
      .from[Try, NonBlank](results.toList, PagingRequest(first, perPage), total)
      .fold(throw _, identity)

  private lazy val onePageOnly: Gen[PagingResponse[NonBlank]] =
    for {
      perPage <- perPages
      results <- nonEmptyList(nonBlankStrings(),
                              minElements = Refined.unsafeApply(perPage.value),
                              maxElements = Refined.unsafeApply(perPage.value))
      total = model.Total(results.size)
    } yield PagingResponse
      .from[Try, NonBlank](results.toList, PagingRequest(first, perPage), total)
      .fold(throw _, identity)
}
