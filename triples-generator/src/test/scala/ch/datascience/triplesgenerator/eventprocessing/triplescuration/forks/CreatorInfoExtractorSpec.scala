/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.Schemas.schema
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.Name
import ch.datascience.rdfstore.JsonLDTriples
import io.renku.jsonld._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CreatorInfoExtractorSpec extends AnyWordSpec with should.Matchers {

  "extract" should {
    "return the names and the email of the creator in the JSONLD triples" in new TestCase {
      val creatorNames = userNames.generateNonEmptyList().toList
      val creatorEmail = userEmails.generateOne

      val creatorJsonLDEntity =
        JsonLD.entity(
          creatorJsonLDEntityId,
          EntityTypes(entityTypes.generateNonEmptyList() :+ EntityType.of(schema / "Person")),
          Map(
            schema / "name"  -> JsonLD.arr(creatorNames.map(name => JsonLD.fromString(name.toString)): _*),
            schema / "email" -> JsonLD.arr(JsonLD.fromString(creatorEmail.toString))
          )
        )

      val json = JsonLD.arr(
        jsonLDEntityWithCreator,
        creatorJsonLDEntity
      )

      CreatorInfoExtratorImpl.extract(JsonLDTriples(json.toJson)) shouldBe (creatorNames, creatorEmail.some)

    }

    "return no email if there are multiple email in the json ld payload" in new TestCase {
      val creatorNames  = userNames.generateNonEmptyList().toList
      val creatorEmails = userEmails.generateNonEmptyList().toList

      val creatorJsonLDEntity =
        JsonLD.entity(
          creatorJsonLDEntityId,
          EntityTypes(entityTypes.generateNonEmptyList() :+ EntityType.of(schema / "Person")),
          Map(
            schema / "name"  -> JsonLD.arr(creatorNames.map(name => JsonLD.fromString(name.toString)): _*),
            schema / "email" -> JsonLD.arr(creatorEmails.map(email => JsonLD.fromString(email.toString)): _*)
          )
        )
      val json = JsonLD.arr(
        jsonLDEntityWithCreator,
        creatorJsonLDEntity
      )

      CreatorInfoExtratorImpl.extract(JsonLDTriples(json.toJson)) shouldBe (creatorNames, None)

    }

    "return an empty list of names if the names cannot be decoded properly" in new TestCase {
      val creatorNames = userNames.generateNonEmptyList().toList
      val creatorEmail = userEmails.generateOne
      val creatorJsonLDEntity =
        JsonLD.entity(
          creatorJsonLDEntityId,
          EntityTypes(entityTypes.generateNonEmptyList() :+ EntityType.of(schema / "Person")),
          Map(
            schema / "name"  -> JsonLD.fromString(creatorNames.head.value),
            schema / "email" -> JsonLD.arr(JsonLD.fromString(creatorEmail.toString))
          )
        )
      val json = JsonLD.arr(
        jsonLDEntityWithCreator,
        creatorJsonLDEntity
      )

      CreatorInfoExtratorImpl.extract(JsonLDTriples(json.toJson)) shouldBe (List.empty[Name], creatorEmail.some)

    }
  }

  private trait TestCase {
    val creatorJsonLDEntityId    = entityIds.generateOne
    val creatorJsonLDIdContainer = JsonLD.fromEntityId(creatorJsonLDEntityId)

    val jsonLDEntityWithCreator = JsonLD.entity(
      entityIds.generateOne,
      entityTypesObject.generateOne,
      valuesProperties
        .generateNonEmptyList()
        .toList
        .toMap
        .updated(schema / "creator", creatorJsonLDIdContainer)
    )
  }
}
