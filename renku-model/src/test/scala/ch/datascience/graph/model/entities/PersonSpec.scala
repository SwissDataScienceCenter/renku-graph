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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.userNames
import ch.datascience.graph.model.Schemas.schema
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.entities.Person.entityTypes
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLD
import io.renku.jsonld.JsonLDEncoder.encodeOption
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {

    "turn JsonLD Person entity into the Person object" in {
      forAll { person: Person =>
        person.asJsonLD.cursor.as[entities.Person] shouldBe person.to[entities.Person].asRight
      }
    }

    "turn JsonLD Person entity with multiple names" in {
      val person     = personEntities().generateOne.to[entities.Person]
      val secondName = userNames.generateOne
      val jsonLDPerson = JsonLD.entity(
        person.resourceId.asEntityId,
        entityTypes,
        schema / "name" -> JsonLD.arr(person.name.asJsonLD, secondName.asJsonLD)
      )

      val Right(personWithSingleName) = jsonLDPerson.cursor.as[entities.Person]
      personWithSingleName.name             should (be(person.name) or be(secondName))
      personWithSingleName.alternativeNames should contain theSameElementsAs List(person.name, secondName)
    }

    "fail if there's no name for a Person" in {
      val person = personEntities().generateOne.to[entities.Person]
      val jsonLDPerson = JsonLD.entity(
        person.resourceId.asEntityId,
        entityTypes,
        schema / "email" -> person.maybeEmail.asJsonLD
      )

      val Left(failure) = jsonLDPerson.cursor.as[entities.Person]
      failure         shouldBe a[DecodingFailure]
      failure.message shouldBe s"No name on Person ${person.resourceId}"
    }
  }
}
