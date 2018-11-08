package ch.datascience.tinytypes

import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.constraints.NonBlank

class NonBlankSpec extends WordSpec with PropertyChecks {

  "NonBlank" should {

    "be instantiatable when values are not blank" in {
      forAll(nonEmptyStrings()) { someValue =>
        NonBlankString(someValue).toString shouldBe someValue.toString
      }
    }

    "throw an IllegalArgumentException for empty String values" in {
      intercept[IllegalArgumentException](NonBlankString("")).getMessage shouldBe "NonBlankString cannot be blank"
    }

    "throw an IllegalArgumentException for blank String values" in {
      intercept[IllegalArgumentException](NonBlankString(" ")).getMessage shouldBe "NonBlankString cannot be blank"
    }
  }
}

private case class NonBlankString(value: String) extends StringValue with NonBlank
