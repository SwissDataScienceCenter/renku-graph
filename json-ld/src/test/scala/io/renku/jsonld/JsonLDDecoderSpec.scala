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

package io.renku.jsonld

import JsonLDDecoder._
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.Cursor.FlattenedJsonCursor
import io.renku.jsonld.JsonLD.JsonLDValue
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators.{localDates, nonEmptyStrings, timestamps}
import io.renku.jsonld.generators.JsonLDGenerators.{entityIds, entityTypesObject, jsonLDEntities, jsonLDValues, properties}
import io.renku.jsonld.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{Instant, LocalDate, ZoneOffset}

class JsonLDDecoderSpec
    extends AnyWordSpec
    with ScalaCheckPropertyChecks
    with should.Matchers
    with TableDrivenPropertyChecks {

  "emap" should {

    "use the given function to map the result" in {
      forAll { value: String =>
        val decoder = decodeString.emap(_.length.asRight[String])
        JsonLD.fromString(value).cursor.as[Int](decoder) shouldBe Right(value.length)
      }
    }

    "fail with the message given in the mapping function" in {
      forAll { value: String =>
        val message = nonEmptyStrings().generateOne
        val decoder = decodeString.emap(_ => message.asLeft[Int])
        JsonLD.fromString(value).cursor.as[Int](decoder).leftMap(_.message) shouldBe Left(message)
      }
    }
  }

  "implicit decoders" should {

    val scenarios = Table(
      ("type", "decoder", "generator", "illegal json value"),
      ("String", decodeString, arbString.arbitrary.map(v => v -> JsonLD.fromString(v)), JsonLD.fromInt(1)),
      ("Long", decodeLong, arbLong.arbitrary.map(v => v -> JsonLD.fromLong(v)), JsonLD.fromString("a")),
      ("Int", decodeInt, arbInt.arbitrary.map(v => v -> JsonLD.fromInt(v)), JsonLD.fromString("a")),
      ("Boolean", decodeBoolean, arbBool.arbitrary.map(v => v -> JsonLD.fromBoolean(v)), JsonLD.fromString("a")),
      ("Instant", decodeInstant, timestamps.map(v => v -> JsonLD.fromInstant(v)), JsonLD.fromInt(1)),
      ("LocalDate", decodeLocalDate, localDates.map(v => v -> JsonLD.fromLocalDate(v)), JsonLD.fromInt(1))
    )

    forAll(scenarios) {
      case (typeName, decoder, generator, illegalType: JsonLDValue[_]) =>
        s"allow to successfully decode $typeName JsonLDValues to $typeName" in {
          forAll(generator) { case (value, jsonLD) =>
            jsonLD.cursor.as(decoder) shouldBe Right(value)
          }
        }

        s"fail to decode JsonLDValues of illegal type to $typeName" in {
          forAll(generator) { case (_, _) =>
            illegalType.cursor.as(decoder) shouldBe Left(
              DecodingFailure(s"Cannot decode ${illegalType.value} to $typeName", Nil)
            )
          }
        }

      case (_, _, _, illegalType) => fail(s"Missing tests for $illegalType")
    }

    "allow to successfully decode Instant from String" in {
      val instant = timestamps.generateOne
      JsonLD.fromString(instant.toString).cursor.as[Instant] shouldBe Right(instant)
    }

    "allow to successfully decode OffsetDateTime from String" in {
      val instant = timestamps.generateOne
      JsonLD.fromString(instant.atOffset(ZoneOffset.ofHours(1)).toString).cursor.as[Instant] shouldBe Right(instant)
    }

    "allow to successfully decode LocalDate from String" in {
      val date = localDates.generateOne
      JsonLD.fromString(date.toString).cursor.as[LocalDate] shouldBe Right(date)
    }

    "allow to successfully decode any JsonLD to JsonLD" in {
      forAll { json: JsonLD =>
        json.cursor.as[JsonLD] shouldBe Right(json)
      }
    }

    "allow to decode to Some for optional property if it exists" in {
      forAll(entityIds, entityTypesObject, properties, Gen.oneOf(jsonLDValues, jsonLDEntities)) {
        (entityId, entityTypes, property, value) =>
          JsonLD
            .entity(entityId, entityTypes, property -> value)
            .cursor
            .downField(property)
            .as[Option[JsonLD]] shouldBe Some(value).asRight
      }
    }

    "allow to decode to None for optional property if it does not exists" in {
      forAll { (entityId: EntityId, entityTypes: EntityTypes) =>
        JsonLD
          .entity(entityId, entityTypes, Map.empty[Property, JsonLD])
          .cursor
          .downField(schema / "non-existing")
          .as[Option[JsonLD]] shouldBe None.asRight
      }
    }
  }

  "decode" should {

    val parent1 = Parent("parent1", Child("parent1child"))
    val parent2 = Parent("parent2", Child("parent2child"))

    "decode not nested entities" in {
      parent1.child.asJsonLD.cursor.as[Child] shouldBe parent1.child.asRight
    }

    "decode nested entities" in {
      parent1.asJsonLD.cursor.as[Parent] shouldBe parent1.asRight
    }

    "decode nested entities - flattened JsonLD case" in {
      parent1.asJsonLD.flatten.fold(fail(_), identity).cursor.as[List[Parent]] shouldBe List(parent1).asRight
    }

    "decode entity with an optional value" in {
      val containerWithNone = OptionalValueContainer(maybeName = None)
      containerWithNone.asJsonLD.cursor.as[OptionalValueContainer] shouldBe containerWithNone.asRight

      val containerWithSome = OptionalValueContainer(maybeName = nonEmptyStrings().generateOne.some)
      containerWithSome.asJsonLD.cursor.as[OptionalValueContainer] shouldBe containerWithSome.asRight
    }

    "decode list of entities" in {
      val jsonLD = JsonLD.arr(parent1.asJsonLD, parent2.asJsonLD)

      parent1.asJsonLD.cursor.as[Parent] shouldBe parent1.asRight

      jsonLD.cursor.as[List[Parent]] shouldBe List(parent1, parent2).asRight
    }

    "decode entity with a property of List of values" in {
      val container = ValuesContainer("container", List("0.0", "0.1", "0.3"))
      container.asJsonLD.cursor.as[ValuesContainer] shouldBe container.asRight
    }

    "decode entity with a single value property when encoded as a single value array" in {
      val childEncoder = JsonLDEncoder.instance[Child](child =>
        JsonLD.entity(EntityId.of(s"child/${child.name}"),
                      EntityTypes.of(schema / "Child"),
                      schema / "name" -> JsonLD.arr(child.name.asJsonLD)
        )
      )

      val parentEncoder = JsonLDEncoder.instance[Parent](parent =>
        JsonLD.entity(parent.id,
                      parent.types,
                      schema / "name"  -> JsonLD.arr(parent.name.asJsonLD),
                      schema / "child" -> JsonLD.arr(parent.child.asJsonLD(childEncoder))
        )
      )

      parent1.asJsonLD(parentEncoder).cursor.as[Parent] shouldBe parent1.asRight
    }

    "decode entity with a single value property when encoded as a single value array - flattened JsonLD case" in {
      val childEncoder = JsonLDEncoder.instance[Child](child =>
        JsonLD.entity(EntityId.of(s"child/${child.name}"),
                      EntityTypes.of(schema / "Child"),
                      schema / "name" -> JsonLD.arr(child.name.asJsonLD)
        )
      )

      val parentEncoder = JsonLDEncoder.instance[Parent](parent =>
        JsonLD.entity(parent.id,
                      parent.types,
                      schema / "name"  -> JsonLD.arr(parent.name.asJsonLD),
                      schema / "child" -> JsonLD.arr(parent.child.asJsonLD(childEncoder))
        )
      )

      parent1.asJsonLD(parentEncoder).flatten.fold(fail(_), identity).cursor.as[List[Parent]] shouldBe
        List(parent1).asRight
    }

    "decode entity when only its id is encoded as a single value array on flattened json" in {
      val childEntityIdEncoder = EntityIdEncoder.instance[Child](child => EntityId.of(s"child/${child.name}"))
      val childEntity = JsonLD.entity(childEntityIdEncoder(parent1.child),
                                      EntityTypes.of(schema / "Child"),
                                      schema / "name" -> JsonLD.arr(parent1.child.name.asJsonLD)
      )

      val parentEntityIdEncoder = EntityIdEncoder.instance[Parent](_.id)
      val parent1Entity = JsonLD.entity(
        parentEntityIdEncoder(parent1),
        parent1.types,
        schema / "name"  -> JsonLD.arr(parent1.name.asJsonLD),
        schema / "child" -> JsonLD.arr(parent1.child.asEntityId(childEntityIdEncoder).asJsonLD)
      )

      val cursor = FlattenedJsonCursor(
        Cursor.Empty(),
        parent1.child.asEntityId(childEntityIdEncoder).asJsonLD,
        allEntities = Map(
          parentEntityIdEncoder(parent1)      -> parent1Entity,
          childEntityIdEncoder(parent1.child) -> childEntity
        )
      )

      cursor.as[List[Child]] shouldBe List(parent1.child).asRight
    }

    "decode entity with an optional value when the value is stored in an array" in {
      val customEncoder = JsonLDEncoder.instance[OptionalValueContainer](container =>
        JsonLD.entity(
          EntityId.of(s"optionalValueContainer/${container.maybeName.getOrElse("container")}"),
          EntityTypes.of(schema / "OptionalValueContainer"),
          schema / "name" -> container.maybeName.map(name => JsonLD.arr(name.asJsonLD)).getOrElse(JsonLD.arr())
        )
      )

      val containerWithNone = OptionalValueContainer(maybeName = None)
      containerWithNone.asJsonLD(customEncoder).cursor.as[OptionalValueContainer] shouldBe containerWithNone.asRight

      val containerWithSome = OptionalValueContainer(maybeName = nonEmptyStrings().generateOne.some)
      containerWithSome.asJsonLD(customEncoder).cursor.as[OptionalValueContainer] shouldBe containerWithSome.asRight
    }

    "decode entity with a property of List of values - case with missing property in JsonLD" in {
      val container = ValuesContainer("container", List())

      val customEncoder = JsonLDEncoder.instance[ValuesContainer](values =>
        JsonLD.entity(
          EntityId.of(s"container/${values.name}"),
          EntityTypes.of(schema / "ValuesContainer"),
          schema / "name" -> values.name.asJsonLD
        )
      )

      container.asJsonLD(customEncoder).cursor.as[ValuesContainer] shouldBe container.asRight
    }

    "decode entity with a property of List of values - case with a single value in JsonLD" in {
      val container = ValuesContainer("container", List("0.0"))

      val customEncoder = JsonLDEncoder.instance[ValuesContainer](values =>
        JsonLD.entity(
          EntityId.of(s"container/${values.name}"),
          EntityTypes.of(schema / "ValuesContainer"),
          schema / "name" -> values.name.asJsonLD,
          schema / "tags" -> values.tags.head.asJsonLD
        )
      )

      container.asJsonLD(customEncoder).cursor.as[ValuesContainer] shouldBe container.asRight
    }

    "decode entity with list of entities" in {
      val parentsContainer = ParentsContainer("parentsContainer", List(parent1, parent2))
      parentsContainer.asJsonLD.cursor.as[ParentsContainer] shouldBe parentsContainer.asRight

      parentsContainer.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[ParentsContainer]] shouldBe List(parentsContainer).asRight
    }

    "decode list of entities if they are flattened" in {
      val jsonLD = JsonLD.arr(parent1.asJsonLD, parent2.asJsonLD).flatten.fold(throw _, identity)

      jsonLD.cursor.as[List[Child]] shouldBe List(parent1.child, parent2.child).asRight

      jsonLD.cursor.as[List[Parent]] shouldBe List(parent1, parent2).asRight
    }

    "decode list of entity ids if they are flattened" in {
      val jsonLD = JsonLD.arr(parent1.asJsonLD, parent2.asJsonLD).flatten.fold(throw _, identity)

      implicit lazy val parentDecoder: JsonLDDecoder[(String, EntityId)] =
        JsonLDDecoder.entity(EntityTypes.of(schema / "Parent")) { cursor =>
          for {
            name  <- cursor.downField(schema / "name").as[String]
            child <- cursor.downField(schema / "child").as[EntityId]
          } yield (name, child)
        }

      jsonLD.cursor.as[List[(String, EntityId)]] shouldBe List(
        (parent1.name, parent1.child.asJsonLD.entityId.getOrElse(fail("No entity id"))),
        (parent2.name, parent2.child.asJsonLD.entityId.getOrElse(fail("No entity id")))
      ).asRight
    }

    "decode a list of list" in {
      val listOfList = ListOfList("2dList", List(List("a1", "a2", "a3"), List("b1", "b2")))
      listOfList.asJsonLD.cursor.as[List[ListOfList]]                                 shouldBe List(listOfList).asRight
      listOfList.asJsonLD.flatten.fold(throw _, identity).cursor.as[List[ListOfList]] shouldBe List(listOfList).asRight
    }

    "decode entity which properties' values are encoded in a single heterogeneous list" in {
      val listContainer = ListContainer("hList", Parent("parent", Child("child2")), Child("child1"))

      listContainer.asJsonLD.cursor.as[ListContainer] shouldBe listContainer.asRight

      JsonLD
        .arr(listContainer.asJsonLD, Child("child3").asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[ListContainer]] shouldBe List(listContainer).asRight
    }

    "decode an entity with fallback - case when fallback has the same types" in {
      lazy val childTraitDecoder = childADecoder orElse childDecoder.widen[ChildTrait]

      val childA = ChildA

      val Right(actual) = childA.asJsonLD.cursor.as(childTraitDecoder)
      actual shouldBe childA
      actual shouldBe a[ChildA.type]

      val Right(List(actualChild)) = JsonLD
        .arr(childA.asJsonLD, ValuesContainer("container", List("1", "2")).asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(childTraitDecoder))

      actualChild shouldBe childA
      actualChild shouldBe a[ChildA.type]

      val childB              = Child("b")
      val Right(actualChildB) = childB.asJsonLD.cursor.as(childTraitDecoder)
      actualChildB shouldBe childB
      actualChildB shouldBe a[Child]
    }

    "decode an entity with fallback - case when fallback has different types in" in {
      lazy val decoder = valuesContainerDecoder orElse childDecoder.widen[Entity]

      val child = Child("b")
      child.asJsonLD.cursor.as(decoder) shouldBe child.asRight

      val values = ValuesContainer("container", List("1", "2"))
      JsonLD
        .arr(child.asJsonLD, values.asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(decoder))
        .map(_.toSet) shouldBe Set(child, values).asRight

      JsonLD
        .arr(child.asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(decoder)) shouldBe List(child).asRight
    }

    "decode an entity if it matches given predicate" in {

      val matchingName = nonEmptyStrings().generateOne
      val predicate: Cursor => JsonLDDecoder.Result[Boolean] = cursor =>
        for {
          types <- cursor.getEntityTypes
          name  <- cursor.downField(schema / "name").as[String]
        } yield types.contains(EntityTypes.of(schema / "Child")) && name == matchingName

      val decoder: JsonLDEntityDecoder[Child] = JsonLDDecoder.entity(EntityTypes.of(schema / "Child"), predicate) {
        _.downField(schema / "name").as[String] map Child.apply
      }

      JsonLD
        .arr(Child(nonEmptyStrings().generateOne).asJsonLD, Child(matchingName).asJsonLD)
        .flatten
        .fold(fail(_), identity)
        .cursor
        .as[List[Child]](decodeList(decoder)) shouldBe List(Child(matchingName)).asRight
    }

    "decode entities with reverse" in {

      val childEncoder = JsonLDEncoder.instance[Child](child =>
        JsonLD.entity(
          EntityId.of(s"child/${child.name}"),
          EntityTypes.of(schema / "Child"),
          Reverse.fromListUnsafe(List(schema / "child" -> parent1.id.asJsonLD)),
          schema / "name" -> child.name.asJsonLD
        )
      )

      val parentEncoder = JsonLDEncoder.instance[Parent](parent =>
        JsonLD.entity(parent.id, parent.types, schema / "name" -> parent.name.asJsonLD)
      )

      JsonLD
        .arr(parent1.asJsonLD(parentEncoder), parent1.child.asJsonLD(childEncoder))
        .flatten
        .fold(fail(_), identity)
        .cursor
        .as[List[Parent]] shouldBe List(parent1).asRight
    }
  }

  private lazy val schema = Schema.from("http://io.renku")

  private sealed trait Entity
  private case class Parent(id: EntityId, types: EntityTypes, name: String, child: Child) extends Entity
  private object Parent {
    val entityTypes: EntityTypes = EntityTypes.of(schema / "Parent")

    def apply(name: String, child: Child): Parent =
      Parent(EntityId.of(s"parent/$name"), entityTypes, name, child)
  }
  private sealed trait ChildTrait extends Entity
  private case class Child(name: String) extends ChildTrait
  private object ChildA extends Child("a")
  private case class OptionalValueContainer(maybeName: Option[String]) extends Entity
  private case class ValuesContainer(name: String, tags: List[String]) extends Entity
  private case class ParentsContainer(name: String, parents: List[Parent])
  private case class ListOfList(name: String, list: List[List[String]])
  private case class ListContainer(name: String, parent: Parent, child: Child)

  private implicit lazy val parentEncoder: JsonLDEncoder[Parent] = JsonLDEncoder.instance(parent =>
    JsonLD.entity(parent.id,
                  parent.types,
                  schema / "name"  -> parent.name.asJsonLD,
                  schema / "child" -> parent.child.asJsonLD
    )
  )

  private implicit lazy val parentDecoder: JsonLDEntityDecoder[Parent] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "Parent")) { cursor =>
      for {
        id    <- cursor.downEntityId.as[EntityId]
        types <- cursor.getEntityTypes
        name  <- cursor.downField(schema / "name").as[String]
        child <- cursor.downField(schema / "child").as[Child]
      } yield Parent(id, types, name, child)
    }

  private implicit lazy val childEncoder: JsonLDEncoder[Child] = JsonLDEncoder.instance(child =>
    JsonLD.entity(EntityId.of(s"child/${child.name}"),
                  EntityTypes.of(schema / "Child"),
                  schema / "name" -> child.name.asJsonLD
    )
  )

  private implicit lazy val childDecoder: JsonLDEntityDecoder[Child] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "Child")) { cursor =>
      cursor.downField(schema / "name").as[String] map Child.apply
    }

  private implicit lazy val childAEncoder: JsonLDEncoder[ChildA.type] = JsonLDEncoder.instance(child =>
    JsonLD.entity(EntityId.of(s"child/${child.name}"),
                  EntityTypes.of(schema / "Child", schema / "ChildA"),
                  schema / "name" -> child.name.asJsonLD
    )
  )

  private lazy val childADecoder: JsonLDEntityDecoder[ChildA.type] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "Child")) { cursor =>
      cursor.downField(schema / "name").as[String] >>= {
        case "a" => Right(ChildA)
        case _   => Left(DecodingFailure("This is not a ChildA", Nil))
      }
    }

  private implicit lazy val optionalValueContainerEncoder: JsonLDEncoder[OptionalValueContainer] =
    JsonLDEncoder.instance(container =>
      JsonLD.entity(
        EntityId.of(s"optionalValueContainer/${container.maybeName.getOrElse("container")}"),
        EntityTypes.of(schema / "OptionalValueContainer"),
        schema / "name" -> container.maybeName.asJsonLD
      )
    )

  private implicit lazy val optionalValueContainerDecoder: JsonLDEntityDecoder[OptionalValueContainer] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "OptionalValueContainer")) { cursor =>
      cursor.downField(schema / "name").as[Option[String]] map OptionalValueContainer.apply
    }

  private implicit lazy val valuesContainerEncoder: JsonLDEncoder[ValuesContainer] = JsonLDEncoder.instance(values =>
    JsonLD.entity(
      EntityId.of(s"container/${values.name}"),
      EntityTypes.of(schema / "ValuesContainer"),
      schema / "name" -> values.name.asJsonLD,
      schema / "tags" -> values.tags.asJsonLD
    )
  )

  private implicit lazy val valuesContainerDecoder: JsonLDEntityDecoder[ValuesContainer] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ValuesContainer")) { cursor =>
      for {
        name <- cursor.downField(schema / "name").as[String]
        tags <- cursor.downField(schema / "tags").as[List[String]]
      } yield ValuesContainer(name, tags)
    }

  private implicit lazy val parentsContainerEncoder: JsonLDEncoder[ParentsContainer] =
    JsonLDEncoder.instance(parentsContainer =>
      JsonLD.entity(
        EntityId.of(s"container/${parentsContainer.name}"),
        EntityTypes.of(schema / "ParentsContainer"),
        schema / "name"    -> parentsContainer.name.asJsonLD,
        schema / "parents" -> parentsContainer.parents.asJsonLD
      )
    )

  private implicit lazy val parentsContainerDecoder: JsonLDDecoder[ParentsContainer] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ParentsContainer")) { cursor =>
      for {
        name    <- cursor.downField(schema / "name").as[String]
        parents <- cursor.downField(schema / "parents").as[List[Parent]]
      } yield ParentsContainer(name, parents)
    }

  private implicit lazy val listOfListEncoder: JsonLDEncoder[ListOfList] =
    JsonLDEncoder.instance(listOfList =>
      JsonLD.entity(
        EntityId.of(s"listOfList/${listOfList.name}"),
        EntityTypes.of(schema / "ListOfList"),
        schema / "name" -> listOfList.name.asJsonLD,
        schema / "list" -> listOfList.list.asJsonLD
      )
    )

  private implicit lazy val listOfListDecoder: JsonLDDecoder[ListOfList] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ListOfList")) { cursor =>
      for {
        name <- cursor.downField(schema / "name").as[String]
        list <- cursor.downField(schema / "list").as[List[List[String]]]
      } yield ListOfList(name, list)
    }

  private implicit lazy val listContainerEncoder: JsonLDEncoder[ListContainer] =
    JsonLDEncoder.instance(container =>
      JsonLD.entity(
        EntityId.of(s"container/${container.name}"),
        EntityTypes.of(schema / "ListContainer"),
        schema / "name" -> container.name.asJsonLD,
        schema / "list" -> JsonLD.arr(container.parent.asJsonLD, container.child.asJsonLD)
      )
    )

  private implicit lazy val listContainerDecoder: JsonLDDecoder[ListContainer] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ListContainer")) { cursor =>
      for {
        name   <- cursor.downField(schema / "name").as[String]
        parent <- cursor.downField(schema / "list").downArray.as[List[Parent]]
        child  <- cursor.downField(schema / "list").downArray.as[List[Child]]
      } yield ListContainer(name, parent.head, child.head)
    }
}
