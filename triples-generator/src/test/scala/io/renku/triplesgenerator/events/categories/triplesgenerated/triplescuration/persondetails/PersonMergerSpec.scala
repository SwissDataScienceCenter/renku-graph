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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Failure, Random, Try}

class PersonMergerSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  import PersonMerger._

  "merge" should {

    "use model values if they don't exist in KG" in {
      val model = personEntities(withGitLabId, withEmail).generateOne
        .to[entities.Person]
        .copy(maybeAffiliation = userAffiliations.generateSome)
      val kg = model.copy(maybeGitLabId = None, maybeEmail = None, maybeAffiliation = None)

      merge[Try](model, kg) shouldBe model.pure[Try]
    }

    "use model values event if they exist in KG" in {
      val model = personEntities(withGitLabId, withEmail).generateOne
        .to[entities.Person]
        .copy(maybeAffiliation = userAffiliations.generateSome)
      val kg = model.copy(maybeGitLabId = userGitLabIds.generateSome,
                          maybeEmail = userEmails.generateSome,
                          maybeAffiliation = userAffiliations.generateSome
      )

      merge[Try](model, kg) shouldBe model.pure[Try]
    }

    "prefer model values if exist" in {
      forAll(personEntities().map(_.to[entities.Person])) { model =>
        val kg = model.copy(
          name = userNames.generateOne,
          maybeGitLabId = userGitLabIds.generateOption,
          maybeEmail = userEmails.generateOption,
          maybeAffiliation = userAffiliations.generateOption
        )

        merge[Try](model, kg) shouldBe model
          .copy(
            maybeGitLabId = model.maybeGitLabId orElse kg.maybeGitLabId,
            maybeEmail = model.maybeEmail orElse kg.maybeEmail,
            maybeAffiliation = model.maybeAffiliation orElse kg.maybeAffiliation,
            alternativeNames = model.alternativeNames + kg.name
          )
          .pure[Try]
      }
    }

    "prefer KG resourceId instead of model resourceId" in {
      val model          = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]
      val emailDifferent = Random.nextBoolean()
      val kg = model.copy(
        resourceId = userResourceIds.generateOne,
        maybeEmail = if (emailDifferent) userEmails.generateSome else model.maybeEmail,
        maybeGitLabId = if (emailDifferent) model.maybeGitLabId else userGitLabIds.generateSome
      )

      merge[Try](model, kg) shouldBe model
        .copy(resourceId = kg.resourceId, alternativeNames = model.alternativeNames + kg.name)
        .pure[Try]
    }

    "fail for is all gitLabId, email or resourceId are different" in {
      val model = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]
      val kg    = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]

      val Failure(error) = merge[Try](model, kg)

      error            shouldBe a[IllegalArgumentException]
      error.getMessage shouldBe s"Persons ${model.resourceId} and ${kg.resourceId} do not have matching identifiers"
    }
  }
}
