package io.renku.jsonld.syntax

import cats.data.NonEmptyList
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class syntaxSpec extends WordSpec with ScalaCheckPropertyChecks {

  "asJsonLD" should {

    "convert a String object into a JsonLD" in {
      forAll { value: String =>
        value.asJsonLD shouldBe JsonLD.fromString(value)
      }
    }

    "convert an Int object into a JsonLD" in {
      forAll { value: Int =>
        value.asJsonLD shouldBe JsonLD.fromInt(value)
      }
    }

    "convert a Long object into a JsonLD" in {
      forAll { value: Long =>
        value.asJsonLD shouldBe JsonLD.fromLong(value)
      }
    }

    "convert a custom value object into a JsonLD" in {
      case class ValueObject(value: String)
      implicit val encoder: JsonLDEncoder[ValueObject] = JsonLDEncoder.instance(o => JsonLD.fromString(o.value))

      forAll { value: String =>
        ValueObject(value).asJsonLD shouldBe JsonLD.fromString(value)
      }
    }

    "convert a custom object into a JsonLD" in {
      val url        = httpUrls.generateOne
      val schema     = schemas.generateOne
      val objectType = schema / nonEmptyStrings().generateOne

      case class Object(string: String, int: Int)
      implicit val encoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/${o.string}"),
          NonEmptyList.of(EntityType.fromProperty(objectType)),
          NonEmptyList.of(
            schema / "string" -> o.string.asJsonLD,
            schema / "int"    -> o.int.asJsonLD
          )
        )
      }

      forAll { (string: String, int: Int) =>
        Object(string, int).asJsonLD shouldBe JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/$string"),
          NonEmptyList.of(EntityType.fromProperty(objectType)),
          NonEmptyList.of(
            schema / "string" -> string.asJsonLD,
            schema / "int"    -> int.asJsonLD
          )
        )
      }
    }

    "convert a custom nested objects into a JsonLD" in {
      val url        = httpUrls.generateOne
      val schema     = schemas.generateOne
      val objectType = schema / nonEmptyStrings().generateOne
      val childType  = schema / nonEmptyStrings().generateOne

      case class Object(string:   String, child: ChildObject)
      case class ChildObject(int: Int)
      implicit val childEncoder: JsonLDEncoder[ChildObject] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/${o.int}"),
          NonEmptyList.of(EntityType.fromProperty(childType)),
          NonEmptyList.of(schema / "int" -> o.int.asJsonLD)
        )
      }
      implicit val objectEncoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/${o.string}"),
          NonEmptyList.of(EntityType.fromProperty(objectType)),
          NonEmptyList.of(
            schema / "string" -> o.string.asJsonLD,
            schema / "child"  -> o.child.asJsonLD
          )
        )
      }

      forAll { (string: String, int: Int) =>
        Object(string, ChildObject(int)).asJsonLD shouldBe JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/$string"),
          NonEmptyList.of(EntityType.fromProperty(objectType)),
          NonEmptyList.of(
            schema / "string" -> string.asJsonLD,
            schema / "child" -> JsonLD.entity(
              EntityId.fromRelativeUri(s"$url/$int"),
              NonEmptyList.of(EntityType.fromProperty(childType)),
              NonEmptyList.of(schema / "int" -> int.asJsonLD)
            )
          )
        )
      }
    }

    "convert a custom nested objects defined by @id only into a JsonLD" in {
      val url        = httpUrls.generateOne
      val schema     = schemas.generateOne
      val objectType = schema / nonEmptyStrings().generateOne

      case class Object(string:   String, child: ChildObject)
      case class ChildObject(int: Int)
      implicit val childEncoder: JsonLDEncoder[ChildObject] = JsonLDEncoder.entityId { o =>
        EntityId.fromRelativeUri(s"$url/${o.int}")
      }
      implicit val objectEncoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/${o.string}"),
          NonEmptyList.of(EntityType.fromProperty(objectType)),
          NonEmptyList.of(
            schema / "string" -> o.string.asJsonLD,
            schema / "child"  -> o.child.asJsonLD
          )
        )
      }

      forAll { (string: String, int: Int) =>
        Object(string, ChildObject(int)).asJsonLD shouldBe JsonLD.entity(
          EntityId.fromRelativeUri(s"$url/$string"),
          NonEmptyList.of(EntityType.fromProperty(objectType)),
          NonEmptyList.of(
            schema / "string" -> string.asJsonLD,
            schema / "child"  -> JsonLD.fromEntityId(EntityId.fromRelativeUri(s"$url/$int"))
          )
        )
      }
    }
  }
}
