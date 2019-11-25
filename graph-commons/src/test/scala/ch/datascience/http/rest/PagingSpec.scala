package ch.datascience.http.rest

import cats.data.Validated
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.http.rest.Paging.PagingRequest
import ch.datascience.http.rest.Paging.PagingRequest.{Page, PerPage}
import ch.datascience.tinytypes.constraints.PositiveInt
import org.http4s.ParseFailure
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PagingSpec extends WordSpec with ScalaCheckPropertyChecks {

  "page" should {

    "decode a valid page query parameter" in {
      forAll { page: PagingRequest.Page =>
        Map("page" -> Seq(page.toString)) match {
          case Paging.PagingRequest.Decoders.page(actual) => actual shouldBe Some(Validated.validNel(page))
        }
      }
    }

    "fail to decode a non-int page query parameter" in {
      Map("page" -> Seq("abc")) match {
        case Paging.PagingRequest.Decoders.page(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'abc' not a valid Page number", "")
          })
      }
    }

    "fail to decode a non-positive page query parameter" in {
      Map("page" -> Seq("0")) match {
        case Paging.PagingRequest.Decoders.page(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'0' not a valid Page number", "")
          })
      }
    }

    "return None when no page query parameter" in {
      Map.empty[String, List[String]] match {
        case Paging.PagingRequest.Decoders.page(actual) => actual shouldBe None
      }
    }
  }

  "perPage" should {

    "decode a valid per_page query parameter" in {
      forAll { perPage: PagingRequest.PerPage =>
        Map("per_page" -> Seq(perPage.toString)) match {
          case Paging.PagingRequest.Decoders.perPage(actual) => actual shouldBe Some(Validated.validNel(perPage))
        }
      }
    }

    "fail to decode a non-int per_page query parameter" in {
      Map("per_page" -> Seq("abc")) match {
        case Paging.PagingRequest.Decoders.perPage(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'abc' not a valid PerPage number", "")
          })
      }
    }

    "fail to decode a non-positive per_page query parameter" in {
      Map("per_page" -> Seq("0")) match {
        case Paging.PagingRequest.Decoders.perPage(actual) =>
          actual shouldBe Some(Validated.invalidNel {
            ParseFailure("'0' not a valid PerPage number", "")
          })
      }
    }

    "return None when no per_page query parameter" in {
      Map.empty[String, List[String]] match {
        case Paging.PagingRequest.Decoders.perPage(actual) => actual shouldBe None
      }
    }
  }

  "Page" should {
    "be a PositiveInt constrained" in {
      Page shouldBe a[PositiveInt]
    }
  }

  "PerPage" should {
    "be a PositiveInt constrained" in {
      PerPage shouldBe a[PositiveInt]
    }
  }

  "PagingRequest.apply" should {

    "instantiate with the given page and perPage" in {
      val page    = pages.generateOne
      val perPage = perPages.generateOne
      PagingRequest(Some(page), Some(perPage)) shouldBe PagingRequest(page, perPage)
    }

    s"default page to ${Page.first} if instantiated with None" in {
      val perPage = perPages.generateOne
      PagingRequest(None, Some(perPage)) shouldBe PagingRequest(Page.first, perPage)
    }

    s"default perPage to ${PerPage.default} if instantiated with None" in {
      val page = pages.generateOne
      PagingRequest(Some(page), None) shouldBe PagingRequest(page, PerPage.default)
    }
  }
}
