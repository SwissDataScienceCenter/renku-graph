package io.renku.jsonld.syntax

import org.scalatest.WordSpec
import org.scalatest.Matchers._
import io.circe.syntax._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.Generators.Implicits._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class syntaxSpec extends WordSpec with ScalaCheckPropertyChecks {

  import io.renku.jsonld.syntax._

  "asJsonLDValue" should {

    "convert a String object into a Json" in {
      forAll(Arbitrary.arbString) { value =>
        value.asJsonLDValue shouldBe value.asJson
      }
    }

    "convert an Int object into a Json" in {
      forAll(Arbitrary.arbInt) { value =>
        value.asJsonLDValue shouldBe value.asJson
      }
    }

    "convert a Long object into a Json" in {
      forAll(Arbitrary.arbLong) { value =>
        value.asJsonLDValue shouldBe value.asJson
      }
    }
  }

//  "asJsonLD" should {
//
//    "convert a String object into a JsonLD" in {
//      forAll(nonBlankStrings().map(_.value)) { value =>
//        value.asJsonLD shouldBe JsonLD.obj("@value" -> value.asJsonLDValue)
//      }
//    }
//  }
}
