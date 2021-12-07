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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AssociationSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {

    "turn JsonLD Association entity with Renku agent into the Association object" in {
      forAll(activityEntities(planEntities())(projectCreatedDates().generateOne).map(_.association)) { association =>
        JsonLD
          .arr(association.asJsonLD, association.plan.asJsonLD)
          .flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Association]] shouldBe List(association.to[entities.Association]).asRight
      }
    }

    "turn JsonLD Association entity with Person agent into the Association object" in {
      val associationWithRenkuAgent =
        activityEntities(planEntities())(projectCreatedDates().generateOne)
          .map(_.association)
          .generateOne
          .to[entities.Association]

      val association: entities.Association =
        entities.Association.WithPersonAgent(associationWithRenkuAgent.resourceId,
                                             personEntities.generateOne.to[entities.Person],
                                             associationWithRenkuAgent.plan
        )

      JsonLD
        .arr(association.asJsonLD, association.plan.asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Association]] shouldBe List(association).asRight
    }
  }
}
