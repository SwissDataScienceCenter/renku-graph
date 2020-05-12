package ch.datascience.tinytypes.ordering

import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.TestTinyTypes.InstantTestType
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TinyTypeOrderingsSpec extends WordSpec with ScalaCheckPropertyChecks {

  import TinyTypeOrderings._

  "compareTo" should {

    "work for InstantTinyTypes" in {
      forAll(timestamps, timestamps) { (instant1, instant2) =>
        InstantTestType(instant1) compareTo InstantTestType(instant2) shouldBe (instant1 compareTo instant2)
      }
    }
  }
}
