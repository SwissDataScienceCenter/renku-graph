package io.renku.jsonld

import io.renku.jsonld.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EntityIdSpec extends WordSpec with ScalaCheckPropertyChecks {

  "of" should {
    "return an EntityId with the given URI in String" in {
      forAll(httpUrls()) { uri =>
        EntityId.of(uri).value shouldBe uri
      }
    }
  }

  "of" should {
    "return an EntityId with the URI created from the given value using the converter" in {
      case class Value(v: String)
      implicit val valueToString: Value => EntityId = v => EntityId.of(v.v)

      forAll(httpUrls() map Value.apply) { value =>
        val entityId = EntityId.of(value)

        entityId       shouldBe a[EntityId]
        entityId.value shouldBe value.v
      }
    }
  }
}
