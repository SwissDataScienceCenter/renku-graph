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

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonLDDecoderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "implicit decoders" should {

    "allow to successfully decode String JsonLDValues to String" in {
      forAll { value: String =>
        JsonLD.fromString(value).cursor.as[String] shouldBe Right(value)
      }
    }

    "fail to decode non-String JsonLDValues to String" in {
      forAll { value: Int =>
        val json = JsonLD.fromInt(value)
        json.cursor.as[String] shouldBe Left(DecodingFailure(s"Cannot decode $json to String", Nil))
      }
    }

    "allow to successfully decode any JsonLD to JsonLD" in {
      forAll { json: JsonLD =>
        json.cursor.as[JsonLD] shouldBe Right(json)
      }
    }
  }

  "encode" should {

    val parent1 = Parent("parent1", Child("parent1child"))
    val parent2 = Parent("parent2", Child("parent2child"))

    "encode not nested entities" in {
      parent1.child.asJsonLD.cursor.as[Child] shouldBe parent1.child.asRight
    }

    "encode nested entities" in {
      parent1.asJsonLD.cursor.as[Parent] shouldBe parent1.asRight
    }

    "encode list of entities" in {
      val jsonLD = JsonLD.arr(parent1.asJsonLD, parent2.asJsonLD)

      parent1.asJsonLD.cursor.as[Parent] shouldBe parent1.asRight

      jsonLD.cursor.as[List[Parent]] shouldBe List(parent1, parent2).asRight
    }

    "encode list of values" in {
      val container = ValuesContainer("container", List("0.0", "0.1", "0.3"))
      container.asJsonLD.cursor.as[ValuesContainer] shouldBe container.asRight
    }

    "encode entity with list of entities" in {
      val parentsContainer = ParentsContainer("parentsContainer", List(parent1, parent2))
      parentsContainer.asJsonLD.cursor.as[ParentsContainer] shouldBe parentsContainer.asRight

      parentsContainer.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[ParentsContainer]] shouldBe List(parentsContainer).asRight
    }

    "encode list of entities if they are flattened" in {
      val jsonLD = JsonLD.arr(parent1.asJsonLD, parent2.asJsonLD).flatten.fold(throw _, identity)

      jsonLD.cursor.as[List[Child]] shouldBe List(parent1.child, parent2.child).asRight

      jsonLD.cursor.as[List[Parent]] shouldBe List(parent1, parent2).asRight
    }

    "encode list of entity ids if they are flattened" in {
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

    "encode a list of list" in {
      val listOfList = ListOfList("2dList", List(List("a1", "a2", "a3"), List("b1", "b2")))
      listOfList.asJsonLD.cursor.as[List[ListOfList]]                                 shouldBe List(listOfList).asRight
      listOfList.asJsonLD.flatten.fold(throw _, identity).cursor.as[List[ListOfList]] shouldBe List(listOfList).asRight
    }

    "encode a list of heterogenous entities" in {
      val containerHList =
        ContainerHList("hList", Parent("parent", Child("child2")), Child("child1"))

      containerHList.asJsonLD.cursor.as[ContainerHList] shouldBe containerHList.asRight

      JsonLD
        .arr(containerHList.asJsonLD, Child("child3").asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[ContainerHList]] shouldBe List(containerHList).asRight
    }
  }

  private case class Parent(name: String, child: Child)
  private case class Child(name: String)
  private case class ValuesContainer(name: String, tags: List[String])
  private case class ParentsContainer(name: String, parents: List[Parent])
  private case class ListOfList(name: String, list: List[List[String]])
  private case class ContainerHList(name: String, parent: Parent, child: Child)

  private val schema = Schema.from("http://io.renku")

  private implicit lazy val parentEncoder: JsonLDEncoder[Parent] = JsonLDEncoder.instance(parent =>
    JsonLD.entity(EntityId.of(s"parent/${parent.name}"),
                  EntityTypes.of(schema / "Parent"),
                  schema / "name"  -> parent.name.asJsonLD,
                  schema / "child" -> parent.child.asJsonLD
    )
  )
  private implicit lazy val childEncoder: JsonLDEncoder[Child] = JsonLDEncoder.instance(child =>
    JsonLD.entity(EntityId.of(s"child/${child.name}"),
                  EntityTypes.of(schema / "Child"),
                  schema / "name" -> child.name.asJsonLD
    )
  )

  private implicit lazy val valuesEncoder: JsonLDEncoder[ValuesContainer] = JsonLDEncoder.instance(values =>
    JsonLD.entity(
      EntityId.of(s"container/${values.name}"),
      EntityTypes.of(schema / "ValuesContainer"),
      schema / "name" -> values.name.asJsonLD,
      schema / "tags" -> values.tags.asJsonLD
    )
  )

  private implicit lazy val parentsContainerEncoder: JsonLDEncoder[ParentsContainer] =
    JsonLDEncoder.instance(parentsContainer =>
      JsonLD.entity(
        EntityId.of(s"container/${parentsContainer.name}"),
        EntityTypes.of(schema / "ParentsContainer"),
        schema / "name"    -> parentsContainer.name.asJsonLD,
        schema / "parents" -> parentsContainer.parents.asJsonLD
      )
    )
  private implicit lazy val listOfListEncoder: JsonLDEncoder[ListOfList] =
    JsonLDEncoder.instance(listOfList =>
      JsonLD.entity(
        EntityId.of(s"listOfList/${listOfList.name}"),
        EntityTypes.of(schema / "ListOfList"),
        schema / "name" -> listOfList.name.asJsonLD,
        schema / "list" -> listOfList.list.asJsonLD
      )
    )

  private implicit lazy val HListEncoder: JsonLDEncoder[ContainerHList] =
    JsonLDEncoder.instance(hList =>
      JsonLD.entity(
        EntityId.of(s"hlist/${hList.name}"),
        EntityTypes.of(schema / "HList"),
        schema / "name" -> hList.name.asJsonLD,
        schema / "list" -> JsonLD.arr(hList.parent.asJsonLD, hList.child.asJsonLD)
      )
    )

  private implicit lazy val parentDecoder: JsonLDDecoder[Parent] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "Parent")) { cursor =>
      for {
        name  <- cursor.downField(schema / "name").as[String]
        child <- cursor.downField(schema / "child").as[Child]
      } yield Parent(name, child)
    }

  private implicit lazy val childDecoder: JsonLDDecoder[Child] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "Child")) { cursor =>
      cursor.downField(schema / "name").as[String] map Child.apply
    }

  private implicit lazy val valuesDecoder: JsonLDDecoder[ValuesContainer] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ValuesContainer")) { cursor =>
      for {
        name <- cursor.downField(schema / "name").as[String]
        tags <- cursor.downField(schema / "tags").as[List[String]]
      } yield ValuesContainer(name, tags)
    }

  private implicit lazy val parentsContainerDecoder: JsonLDDecoder[ParentsContainer] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ParentsContainer")) { cursor =>
      for {
        name    <- cursor.downField(schema / "name").as[String]
        parents <- cursor.downField(schema / "parents").as[List[Parent]]
      } yield ParentsContainer(name, parents)
    }

  private implicit lazy val listOfListDecoder: JsonLDDecoder[ListOfList] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "ListOfList")) { cursor =>
      for {
        name <- cursor.downField(schema / "name").as[String]
        list <- cursor.downField(schema / "list").as[List[List[String]]]
      } yield ListOfList(name, list)
    }

  private implicit lazy val hListDecoder: JsonLDDecoder[ContainerHList] =
    JsonLDDecoder.entity(EntityTypes.of(schema / "HList")) { cursor =>
      for {
        name   <- cursor.downField(schema / "name").as[String]
        parent <- cursor.downField(schema / "list").downArray.as[List[Parent]]
        child  <- cursor.downField(schema / "list").downArray.as[List[Child]]
      } yield ContainerHList(name, parent.head, child.head)
    }

}
