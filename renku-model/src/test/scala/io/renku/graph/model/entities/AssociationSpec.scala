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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.{Graph, entities}
import io.renku.graph.model.testentities._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AssociationSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {
    implicit val graph: Graph = Graph.Default

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

  "encode for the Default Graph" should {
    implicit val graph: Graph = Graph.Default

    "produce JsonLD with all the relevant properties" in {
      val associationWithRenkuAgent =
        activityEntities(planEntities())(projectCreatedDates().generateOne)
          .map(_.association)
          .generateOne
          .to[entities.Association]

      val agent = personEntities.generateOne
      val association: entities.Association = entities.Association.WithPersonAgent(associationWithRenkuAgent.resourceId,
                                                                                   agent.to[entities.Person],
                                                                                   associationWithRenkuAgent.plan
      )

      association.asJsonLD shouldBe JsonLD.entity(
        association.resourceId.asEntityId,
        entities.Association.entityTypes,
        prov / "agent"   -> agent.asJsonLD,
        prov / "hadPlan" -> association.plan.resourceId.asEntityId.asJsonLD
      )
    }
  }

  "encode for the Project Graph" should {
    implicit val graph: Graph = Graph.Project

    "produce JsonLD with all the relevant properties and only links to Person entities" in {
      val associationWithRenkuAgent =
        activityEntities(planEntities())(projectCreatedDates().generateOne)
          .map(_.association)
          .generateOne
          .to[entities.Association]

      val agent = personEntities.generateOne
      val association: entities.Association = entities.Association.WithPersonAgent(associationWithRenkuAgent.resourceId,
                                                                                   agent.to[entities.Person],
                                                                                   associationWithRenkuAgent.plan
      )

      association.asJsonLD shouldBe JsonLD.entity(
        association.resourceId.asEntityId,
        entities.Association.entityTypes,
        prov / "agent"   -> agent.asEntityId.asJsonLD,
        prov / "hadPlan" -> association.plan.resourceId.asEntityId.asJsonLD
      )
    }
  }
}
