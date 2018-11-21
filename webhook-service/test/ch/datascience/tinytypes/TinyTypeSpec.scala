package ch.datascience.tinytypes

import org.scalatest.Matchers._
import org.scalatest.WordSpec

class TinyTypeSpec extends WordSpec {

  "toString" should {

    "return a String value of the 'value' property" in {
      ( "abc" +: 2 +: 2L +: true +: Nil ) foreach { someValue =>
        val tinyType: TinyType[Any] = new TinyType[Any] {
          override val value: Any = someValue
        }

        tinyType.toString shouldBe someValue.toString
      }
    }
  }
}
