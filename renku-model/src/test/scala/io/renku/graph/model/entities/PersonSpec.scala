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
import io.circe.DecodingFailure
import io.renku.cli.model.CliPerson
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.Person.{entityTypes, gitLabIdEncoder, orcidIdEncoder}
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.Person
import io.renku.graph.model.{GraphClass, GraphModelGenerators, entities, persons}
import io.renku.jsonld.syntax._
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks with EntitiesGenerators {

  (GraphClass.Default :: GraphClass.Persons :: Nil) foreach { implicit graph =>
    show"encode as an Entity for the $graph Graph" should {

      "use the Person resourceId if there's no GitLabId" in {
        val person = personEntities(withoutGitLabId).generateOne.to[entities.Person]
        person.asJsonLD.cursor.downEntityId.as[persons.ResourceId] shouldBe person.resourceId.asRight
      }

      "use the resourceId based on person's GitLabId if it exists" in {
        val gitLabId = personGitLabIds.generateOne
        forAll(personEntities().map(_.copy(maybeGitLabId = gitLabId.some))) { person =>
          person.asJsonLD.cursor.downEntityId.as[persons.ResourceId] shouldBe persons.ResourceId(gitLabId).asRight
        }
      }

      "use the resourceId based on person's GitLabId if it exists even if there's an orcidId" in {
        val gitLabId = personGitLabIds.generateOne
        val person = personEntities()
          .map(_.copy(maybeGitLabId = gitLabId.some, maybeOrcidId = personOrcidIds.generateSome))
          .generateOne
        person.asJsonLD.cursor.downEntityId.as[persons.ResourceId] shouldBe persons.ResourceId(gitLabId).asRight
      }
    }
  }

  "encode as entityId only for the Project Graph" should {
    implicit val graph: GraphClass = GraphClass.Project

    "use the Person resourceId if there's no GitLabId" in {
      val person = personEntities().generateOne.to[entities.Person]
      person.asJsonLD shouldBe person.resourceId.asEntityId.asJsonLD
    }
  }

  "decode" should {

    "turn JsonLD Person entity into the Person object" in {
      forAll(cliShapedPersons) { person: Person =>
        person.to[CliPerson].asFlattenedJsonLD.cursor.as[entities.Person] shouldBe person.to[entities.Person].asRight
      }
    }

    "turn JsonLD Person entity with multiple names" in {
      val resourceId = personNameResourceId.generateOne
      val firstName  = personNames.generateOne
      val secondName = personNames.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        schema / "name" -> JsonLD.arr(firstName.asJsonLD, secondName.asJsonLD)
      )

      val Right(person) = jsonLDPerson.cursor.as[entities.Person]

      person.name should (be(firstName) or be(secondName))
    }

    "take the last Affiliation in case of multiple for a Person" in {
      val resourceId   = personNameResourceId.generateOne
      val name         = personNames.generateOne
      val affiliation1 = personAffiliations.generateOne
      val affiliation2 = personAffiliations.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        schema / "name"        -> name.asJsonLD,
        schema / "affiliation" -> JsonLD.arr(affiliation1.asJsonLD, affiliation2.asJsonLD)
      )

      jsonLDPerson.cursor.as[entities.Person].map(_.maybeAffiliation) shouldBe affiliation2.some.asRight
    }

    "fail if person's ResourceId does not match the GitLabId based ResourceId if GitLabId given" in {
      forAll(Gen.oneOf(personEmailResourceId, personNameResourceId).widen[persons.ResourceId],
             personGitLabIds,
             personEmails.toGeneratorOfOptions,
             personNames
      ) { (invalidResourceId, gitLabId, maybeEmail, name) =>
        val jsonLDPerson = JsonLD.entity(
          invalidResourceId.asEntityId,
          entityTypes,
          schema / "name"   -> name.asJsonLD,
          schema / "email"  -> maybeEmail.asJsonLD,
          schema / "sameAs" -> gitLabId.asJsonLD(gitLabIdEncoder)
        )

        val Left(failure) = jsonLDPerson.cursor.as[entities.Person]

        failure shouldBe a[DecodingFailure]
        failure.message shouldBe
          show"Invalid Person: ${invalidResourceId.asEntityId}, name = $name, gitLabId = ${gitLabId.some}, " +
          show"orcidId = ${Option.empty[persons.OrcidId]}, email = $maybeEmail, affiliation = None"
      }
    }

    "fail if person's ResourceId does not match the OrcidId based ResourceId if OrcidId given" in {
      forAll(personOrcidResourceId.widen[persons.ResourceId], personOrcidIds, personNames) {
        (invalidResourceId, orcidId, name) =>
          val jsonLDPerson = JsonLD.entity(
            invalidResourceId.asEntityId,
            entityTypes,
            schema / "name"   -> name.asJsonLD,
            schema / "sameAs" -> orcidId.asJsonLD(orcidIdEncoder)
          )

          val Left(failure) = jsonLDPerson.cursor.as[entities.Person]

          failure shouldBe a[DecodingFailure]
          failure.message shouldBe
            show"Invalid Person: ${invalidResourceId.asEntityId}, name = $name, gitLabId = ${Option.empty[persons.GitLabId]}, " +
            show"orcidId = ${orcidId.some}, email = ${Option.empty[persons.Email]}, affiliation = None"
      }
    }

    "fail if person's ResourceId does not match the Email or Orcid based ResourceId if no GitLabId but Email given" in {
      forAll(Gen.oneOf(personGitLabResourceId, personNameResourceId).widen[persons.ResourceId],
             personEmails,
             personNames
      ) { (invalidResourceId, email, name) =>
        val jsonLDPerson = JsonLD.entity(
          invalidResourceId.asEntityId,
          entityTypes,
          schema / "name"  -> name.asJsonLD,
          schema / "email" -> email.asJsonLD
        )

        val Left(failure) = jsonLDPerson.cursor.as[entities.Person]

        failure shouldBe a[DecodingFailure]
        failure.message shouldBe
          show"Invalid Person: ${invalidResourceId.asEntityId}, name = $name, gitLabId = ${Option.empty[persons.GitLabId]}, " +
          show"orcidId = ${Option.empty[persons.OrcidId]}, email = ${email.some}, affiliation = None"
      }
    }

    "fail if person's ResourceId does not match the Name or Orcid based ResourceId if no GitLabId and Email given" in {
      forAll(Gen.oneOf(personGitLabResourceId, personEmailResourceId).widen[persons.ResourceId], personNames) {
        (invalidResourceId, name) =>
          val jsonLDPerson = JsonLD.entity(
            invalidResourceId.asEntityId,
            entityTypes,
            schema / "name" -> name.asJsonLD
          )

          val Left(failure) = jsonLDPerson.cursor.as[entities.Person]

          failure shouldBe a[DecodingFailure]
          failure.message shouldBe
            show"Invalid Person: ${invalidResourceId.asEntityId}, name = $name, gitLabId = ${Option.empty[persons.GitLabId]}, " +
            show"orcidId = ${Option.empty[persons.OrcidId]}, email = ${Option.empty[persons.Email]}, affiliation = None"
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
