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
import ch.datascience.graph.model.GraphModelGenerators.userEmails
import ch.datascience.graph.model.Schemas.schema
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.entities.Person.entityTypes
import ch.datascience.graph.model.testentities._
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

    "turn JsonLD Person entity with multiple emails into the Person object with a single email" in {
      val person = personEntities(maybeEmails = withEmail).generateOne.to[entities.Person]

      val secondEmail = userEmails.generateOne.asJsonLD
      val jsonLDPerson = JsonLD.entity(
        person.resourceId.asEntityId,
        entityTypes,
        schema / "email" -> JsonLD.arr(person.maybeEmail.asJsonLD, secondEmail),
        schema / "name"  -> person.name.asJsonLD
      )

      val Right(personWithSingleEmail) = jsonLDPerson.cursor.as[entities.Person]
      personWithSingleEmail.maybeEmail should (be(person.maybeEmail) or be(secondEmail.some))
    }
  }
}
