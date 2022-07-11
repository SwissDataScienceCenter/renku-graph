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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.persondetails

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Failure, Try}

class PersonMergerSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  import PersonMerger._

  "merge" should {

    "fail if both objects have different ResourceIds" in {
      val model = personEntities(withGitLabId).generateOne.to[entities.Person]
      val kg    = model.add(personGitLabIds.generateOne)

      val Failure(error) = merge[Try](model, kg)

      error            shouldBe a[IllegalArgumentException]
      error.getMessage shouldBe s"Persons ${model.resourceId} and ${kg.resourceId} do not have matching identifiers"
    }

    "prefer model values if they exist in case of Person with GitLabId" in {
      forAll(personEntities(withGitLabId) map (_.toMaybe[entities.Person.WithGitLabId])) {
        case Some(model) =>
          val kg = model.copy(
            name = personNames.generateOne,
            maybeEmail = personEmails.generateOption,
            maybeAffiliation = personAffiliations.generateOption
          )

          merge[Try](model, kg) shouldBe model
            .copy(
              maybeEmail = model.maybeEmail orElse kg.maybeEmail,
              maybeAffiliation = model.maybeAffiliation orElse kg.maybeAffiliation
            )
            .pure[Try]
        case None => fail("Cannot convert to entities.person")
      }
    }

    "prefer model values if they exist in case of Person with Email but no GitLabId" in {
      forAll(personEntities(withoutGitLabId, withEmail) map (_.toMaybe[entities.Person.WithEmail])) {
        case Some(model) =>
          val kg = model.copy(
            name = personNames.generateOne,
            maybeAffiliation = personAffiliations.generateOption
          )

          merge[Try](model, kg) shouldBe model
            .copy(
              maybeAffiliation = model.maybeAffiliation orElse kg.maybeAffiliation
            )
            .pure[Try]
        case None => fail("Cannot convert to entities.person")
      }
    }

    "prefer model values if they exist in case of Person without Email and GitLabId" in {
      forAll(personEntities(withoutGitLabId, withoutEmail) map (_.toMaybe[entities.Person.WithNameOnly])) {
        case Some(model) =>
          val kg = model.copy(
            name = personNames.generateOne,
            maybeAffiliation = personAffiliations.generateOption
          )

          merge[Try](model, kg) shouldBe model
            .copy(
              maybeAffiliation = model.maybeAffiliation orElse kg.maybeAffiliation
            )
            .pure[Try]
        case None => fail("Cannot convert to entities.person")
      }
    }
  }
}
