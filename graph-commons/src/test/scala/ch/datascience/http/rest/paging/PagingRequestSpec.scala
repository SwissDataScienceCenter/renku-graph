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

package ch.datascience.http.rest.paging

import cats.data.{NonEmptyList, Validated}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.rest.paging.model.{Page, PerPage}
import org.http4s.ParseFailure
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PagingRequestSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "page" should {

    "decode a valid page query parameter" in {
      forAll { page: Page =>
        Map("page" -> Seq(page.toString)) match {
          case PagingRequest.Decoders.page(actual) => actual shouldBe Some(Validated.validNel(page))
        }
      }
    }

    "fail to decode a non-int page query parameter" in {
      Map("page" -> Seq("abc")) match {
        case PagingRequest.Decoders.page(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'abc' not a valid Page number", "")
          })
      }
    }

    "fail to decode a non-positive page query parameter" in {
      Map("page" -> Seq("0")) match {
        case PagingRequest.Decoders.page(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'0' not a valid Page number", "")
          })
      }
    }

    "return None when no page query parameter" in {
      Map.empty[String, List[String]] match {
        case PagingRequest.Decoders.page(actual) => actual shouldBe None
      }
    }
  }

  "perPage" should {

    "decode a valid per_page query parameter" in {
      forAll { perPage: PerPage =>
        Map("per_page" -> Seq(perPage.toString)) match {
          case PagingRequest.Decoders.perPage(actual) => actual shouldBe Some(Validated.validNel(perPage))
        }
      }
    }

    "fail to decode a non-int per_page query parameter" in {
      Map("per_page" -> Seq("abc")) match {
        case PagingRequest.Decoders.perPage(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'abc' not a valid PerPage number", "")
          })
      }
    }

    "fail to decode a non-positive per_page query parameter" in {
      Map("per_page" -> Seq("0")) match {
        case PagingRequest.Decoders.perPage(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'0' not a valid PerPage number", "")
          })
      }
    }

    "return None when no per_page query parameter" in {
      Map.empty[String, List[String]] match {
        case PagingRequest.Decoders.perPage(actual) => actual shouldBe None
      }
    }
  }

  "PagingRequest.apply" should {

    "instantiate with the given page and perPage" in {
      val page    = pages.generateOne
      val perPage = perPages.generateOne
      PagingRequest(Some(page.validNel), Some(perPage.validNel)) shouldBe PagingRequest(page, perPage).validNel
    }

    s"default page to ${Page.first} if instantiated with None" in {
      val perPage = perPages.generateOne
      PagingRequest(None, Some(perPage.validNel)) shouldBe PagingRequest(Page.first, perPage).validNel
    }

    s"default perPage to ${PerPage.default} if instantiated with None" in {
      val page = pages.generateOne
      PagingRequest(Some(page.validNel), None) shouldBe PagingRequest(page, PerPage.default).validNel
    }

    "return the failure if exists for the page" in {
      val pageParsingError = parseFailures.generateOne.invalidNel[Page]
      PagingRequest(Some(pageParsingError), None) shouldBe pageParsingError
    }

    "return the failure if exists for the perPage" in {
      val perPageParsingError = parseFailures.generateOne.invalidNel[PerPage]
      PagingRequest(None, Some(perPageParsingError)) shouldBe perPageParsingError
    }

    "return merged failures if exists for the page and perPage" in {
      val pageParsingError    = parseFailures.generateOne
      val perPageParsingError = parseFailures.generateOne

      PagingRequest(Some(pageParsingError.invalidNel[Page]),
                    Some(perPageParsingError.invalidNel[PerPage])
      ) shouldBe Validated
        .Invalid(NonEmptyList.of(pageParsingError, perPageParsingError))
    }
  }

  private val parseFailures: Gen[ParseFailure] = sentences() map (v => ParseFailure(v.value, ""))
}
