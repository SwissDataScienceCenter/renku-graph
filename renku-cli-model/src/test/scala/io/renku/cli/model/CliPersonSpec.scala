/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.PersonGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliPersonSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers {

  private implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.generateOne
  private val personGen = PersonGenerators.cliPersonGen

  "decode/encode" should {
    "be compatible" in {
      forAll(personGen) { cliPerson =>
        assertCompatibleCodec(cliPerson)
      }
    }

    "work on multiple items" in {
      forAll(personGen, personGen) { (cliPerson1, cliPerson2) =>
        assertCompatibleCodec(cliPerson1, cliPerson2)
      }
    }

    "fail when there is no name" in {
      forAll(personGen) { person =>
        val jsonLDPerson = JsonLD.entity(
          person.resourceId.asEntityId,
          CliPerson.entityTypes,
          Ontologies.Schema.email -> person.email.asJsonLD
        )

        val Left(failure) = jsonLDPerson.cursor.as[CliPerson]
        failure         shouldBe a[DecodingFailure]
        failure.message shouldBe show"No name on Person ${person.resourceId}"
      }
    }

    "support multiple names, picking any" in {
      val resourceId = RenkuTinyTypeGenerators.personNameResourceId.generateOne
      val firstName  = RenkuTinyTypeGenerators.personNames.generateOne
      val secondName = RenkuTinyTypeGenerators.personNames.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        CliPerson.entityTypes,
        Ontologies.Schema.name -> JsonLD.arr(firstName.asJsonLD, secondName.asJsonLD)
      )

      val Right(person) = jsonLDPerson.cursor.as[CliPerson]

      person.name should (be(firstName) or be(secondName))
    }

    "support multiple affiliations, picking last" in {
      val resourceId   = RenkuTinyTypeGenerators.personNameResourceId.generateOne
      val name         = RenkuTinyTypeGenerators.personNames.generateOne
      val affiliation1 = RenkuTinyTypeGenerators.personAffiliations.generateOne
      val affiliation2 = RenkuTinyTypeGenerators.personAffiliations.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        CliPerson.entityTypes,
        Ontologies.Schema.name        -> name.asJsonLD,
        Ontologies.Schema.affiliation -> List(affiliation1, affiliation2).asJsonLD
      )

      jsonLDPerson.cursor.as[CliPerson].map(_.affiliation) shouldBe affiliation2.some.asRight
    }
  }
}
