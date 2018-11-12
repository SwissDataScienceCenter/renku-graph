package ch.datascience.http

import java.net.URLEncoder

import akka.http.scaladsl.model.Uri
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class UriOpsSpec extends WordSpec with PropertyChecks {

  import UriOps._

  "/" should {

    "append the given segment to the uri" in {
      forAll(nonEmptyStrings()) { path =>
        Uri("some-uri") / path shouldBe Uri(s"some-uri/$path")
      }
    }

    "uri encode and append the given segment to the uri" in {
      val path = s"${nonEmptyStrings().generateOne}.md"
      Uri("some-uri") / path shouldBe Uri(s"some-uri/${urlEncode(path)}")
    }
  }

  "&" should {

    "add query parameter if there's any" in {
      forAll(nonEmptyStrings(), Gen.oneOf(1, "string")) { (key, value) =>
        Uri("some-uri") & (key, value) shouldBe Uri(s"some-uri?$key=$value")
      }
    }

    "add query parameter if there are some already" in {
      val key = nonEmptyStrings().generateOne
      val value = nonEmptyStrings().generateOne
      Uri("some-uri?param=value") & (key, value) shouldBe Uri(s"some-uri?param=value&$key=$value")
    }
  }

  private val urlEncode: String => String = URLEncoder.encode(_, "UTF-8")
}
