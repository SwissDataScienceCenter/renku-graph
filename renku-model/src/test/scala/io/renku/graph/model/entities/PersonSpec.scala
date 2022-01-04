/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.Person.{entityTypes, gitLabIdEncoder}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, users}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "encode" should {

    "use the Person resourceId if there's no GitLabId" in {
      val person = personEntities(withoutGitLabId).generateOne.to[entities.Person]
      person.asJsonLD.cursor.downEntityId.as[users.ResourceId] shouldBe person.resourceId.asRight
    }

    "use the resourceId based on person's GitLabId if it exists" in {
      val gitLabId = userGitLabIds.generateOne
      forAll(personEntities().map(_.copy(maybeGitLabId = gitLabId.some))) { person =>
        person.asJsonLD.cursor.downEntityId.as[users.ResourceId] shouldBe users.ResourceId(gitLabId).asRight
      }
    }
  }

  "decode" should {

    "turn JsonLD Person entity into the Person object" in {
      forAll { person: Person =>
        person.asJsonLD.cursor.as[entities.Person] shouldBe person.to[entities.Person].asRight
      }
    }

    "turn JsonLD Person entity with multiple names" in {
      val resourceId = userNameResourceId.generateOne
      val firstName  = userNames.generateOne
      val secondName = userNames.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        schema / "name" -> JsonLD.arr(firstName.asJsonLD, secondName.asJsonLD)
      )

      val Right(person) = jsonLDPerson.cursor.as[entities.Person]

      person.name should (be(firstName) or be(secondName))
    }

    "take the last Affiliation in case of multiple for a Person" in {
      val resourceId   = userNameResourceId.generateOne
      val name         = userNames.generateOne
      val affiliation1 = userAffiliations.generateOne
      val affiliation2 = userAffiliations.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        schema / "name"        -> name.asJsonLD,
        schema / "affiliation" -> JsonLD.arr(affiliation1.asJsonLD, affiliation2.asJsonLD)
      )

      jsonLDPerson.cursor.as[entities.Person].map(_.maybeAffiliation) shouldBe affiliation2.some.asRight
    }

    "fail if there's no name for a Person" in {
      val resourceId = userResourceIds.generateOne
      val jsonLDPerson = JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        schema / "email" -> userEmails.generateOne.asJsonLD
      )

      val Left(failure) = jsonLDPerson.cursor.as[entities.Person]
      failure         shouldBe a[DecodingFailure]
      failure.message shouldBe show"No name on Person $resourceId"
    }

    "fail if person's ResourceId does not match the GitLabId based ResourceId if GitLabId given" in {
      forAll(Gen.oneOf(userEmailResourceId, userNameResourceId).widen[users.ResourceId],
             userGitLabIds,
             userEmails.toGeneratorOfOptions
      ) { (invalidResourceId, gitLabId, maybeEmail) =>
        val jsonLDPerson = JsonLD.entity(
          invalidResourceId.asEntityId,
          entityTypes,
          schema / "name"   -> userNames.generateOne.asJsonLD,
          schema / "email"  -> maybeEmail.asJsonLD,
          schema / "sameAs" -> gitLabId.asJsonLD(gitLabIdEncoder)
        )

        val Left(failure) = jsonLDPerson.cursor.as[entities.Person]
        failure shouldBe a[DecodingFailure]
        failure.message shouldBe show"Invalid Person with $invalidResourceId, gitLabId = ${gitLabId.some}, email = $maybeEmail"
      }
    }

    "fail if person's ResourceId does not match the Email based ResourceId if no GitLabId but Email given" in {
      forAll(Gen.oneOf(userGitLabResourceId, userNameResourceId).widen[users.ResourceId], userEmails) {
        (invalidResourceId, email) =>
          val jsonLDPerson = JsonLD.entity(
            invalidResourceId.asEntityId,
            entityTypes,
            schema / "name"  -> userNames.generateOne.asJsonLD,
            schema / "email" -> email.asJsonLD
          )

          val Left(failure) = jsonLDPerson.cursor.as[entities.Person]
          failure shouldBe a[DecodingFailure]
          failure.message shouldBe show"Invalid Person with $invalidResourceId, " +
            show"gitLabId = ${Option.empty[users.GitLabId]}, email = ${email.some}"
      }
    }

    "fail if person's ResourceId does not match the Name based ResourceId if no GitLabId and Email given" in {
      forAll(Gen.oneOf(userGitLabResourceId, userEmailResourceId).widen[users.ResourceId]) { invalidResourceId =>
        val jsonLDPerson = JsonLD.entity(
          invalidResourceId.asEntityId,
          entityTypes,
          schema / "name" -> userNames.generateOne.asJsonLD
        )

        val Left(failure) = jsonLDPerson.cursor.as[entities.Person]
        failure shouldBe a[DecodingFailure]
        failure.message shouldBe show"Invalid Person with $invalidResourceId, " +
          show"gitLabId = ${Option.empty[users.GitLabId]}, email = ${Option.empty[users.Email]}"
      }
    }
  }
}
