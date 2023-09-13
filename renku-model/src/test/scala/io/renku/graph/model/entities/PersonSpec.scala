/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.renku.cli.model.{CliPerson, CliPersonResourceId}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.Person
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.{GraphClass, GraphModelGenerators, entities, persons}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EntitiesGenerators
    with AdditionalMatchers
    with DiffInstances {

  "encode as entityId only for the Project Graph" should {
    implicit val graph: GraphClass = GraphClass.Project

    "use the Person resourceId if there's no GitLabId" in {
      val person = personEntities().generateOne.to[entities.Person]
      person.asJsonLD shouldBe person.resourceId.asEntityId.asJsonLD
    }
  }

  "apply(Name, GitLabId)" should {

    "instantiate a Person.WithGitLabId" in {
      forAll(personNames, personGitLabIds) { (name, glId) =>
        entities.Person(name, glId) shouldBe entities.Person.WithGitLabId(persons.ResourceId(glId),
                                                                          glId,
                                                                          name,
                                                                          maybeEmail = None,
                                                                          maybeOrcidId = None,
                                                                          maybeAffiliation = None
        )
      }
    }
  }

  "fromCli" should {

    "turn CliPerson entity into the Person object" in {
      forAll(cliShapedPersons) { person: Person =>
        val cliPerson = person.to[CliPerson]
        entities.Person.fromCli(cliPerson) shouldMatchToValid person.to[entities.Person]
      }
    }

    "fail if person's ResourceId does not match the Email or Orcid based ResourceId if no GitLabId but Email given" in {
      forAll(Gen.oneOf(personGitLabResourceId, personNameResourceId).widen[persons.ResourceId],
             personEmails,
             personNames
      ) { (invalidResourceId, email, name) =>
        val cliPerson = CliPerson(CliPersonResourceId(invalidResourceId.value), name, email.some, None)
        val result    = entities.Person.fromCli(cliPerson)
        result should beInvalidWithMessageIncluding(
          show"Invalid Person: ${invalidResourceId.asEntityId}, name = $name, gitLabId = ${Option.empty[persons.GitLabId]}, " +
            show"orcidId = ${Option.empty[persons.OrcidId]}, email = ${email.some}, affiliation = None"
        )
      }
    }

    "fail if person's ResourceId does not match the Name or Orcid based ResourceId if no GitLabId and Email given" in {
      forAll(Gen.oneOf(personGitLabResourceId, personEmailResourceId).widen[persons.ResourceId], personNames) {
        (invalidResourceId, name) =>
          val cliPerson = CliPerson(CliPersonResourceId(invalidResourceId.value), name, None, None)
          val result    = entities.Person.fromCli(cliPerson)
          result should beInvalidWithMessageIncluding(
            show"Invalid Person: ${invalidResourceId.asEntityId}, name = $name, gitLabId = ${Option.empty[persons.GitLabId]}, " +
              show"orcidId = ${Option.empty[persons.OrcidId]}, email = ${Option.empty[persons.Email]}, affiliation = None"
          )
      }
    }
  }

  "entityFunctions.findAllPersons" should {

    "return itself" in {

      val person = personEntities.generateOne.to[entities.Person]

      EntityFunctions[entities.Person].findAllPersons(person) shouldBe Set(person)
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val person = personEntities.generateOne.to[entities.Person]

      implicit val graph: GraphClass = GraphModelGenerators.graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Person].encoder(graph)

      person.asJsonLD(functionsEncoder) shouldBe person.asJsonLD
    }
  }
}
