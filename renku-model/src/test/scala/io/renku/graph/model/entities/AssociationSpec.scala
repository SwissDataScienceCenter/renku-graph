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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, plans}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDDecoder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AssociationSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD Association entity with Renku agent into the Association object" in {
      forAll(activityEntities(stepPlanEntities())(projectCreatedDates().generateOne).map(_.association)) {
        association =>
          implicit val decoder: JsonLDDecoder[entities.Association] =
            createDecoder(association.plan.to[entities.StepPlan])

          association.asJsonLD.flatten.fold(throw _, identity).cursor.as[List[entities.Association]] shouldBe
            List(association.to[entities.Association]).asRight
      }
    }

    "turn JsonLD Association entity with Person agent into the Association object" in {

      val (association, _, plan) = generateAssociationWithPersonAgent

      implicit val decoder: JsonLDDecoder[entities.Association] = createDecoder(plan)

      association.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Association]] shouldBe List(association).asRight
    }

    "fail decoding if there's no Plan with the Id the Association points to" in {

      val association =
        activityEntities(stepPlanEntities())(projectCreatedDates().generateOne)
          .map(_.association)
          .generateOne
          .to[entities.Association]

      implicit val dl: DependencyLinks = (_: plans.ResourceId) => Option.empty[entities.StepPlan]

      val Left(failure) = association.asJsonLD.flatten.fold(throw _, identity).cursor.as[List[entities.Association]]

      failure.message should include(
        show"Association ${association.resourceId} points to a non-existing Plan ${association.planId}"
      )
    }
  }

  "encode for the Default Graph" should {
    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD with all the relevant properties" in {

      val (association, agent, _) = generateAssociationWithPersonAgent

      association.asJsonLD shouldBe JsonLD.entity(
        association.resourceId.asEntityId,
        entities.Association.entityTypes,
        prov / "agent"   -> agent.asJsonLD,
        prov / "hadPlan" -> association.planId.asEntityId.asJsonLD
      )
    }
  }

  "encode for the Project Graph" should {
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD with all the relevant properties and only links to Person entities" in {

      val (association, agent, _) = generateAssociationWithPersonAgent

      association.asJsonLD shouldBe JsonLD.entity(
        association.resourceId.asEntityId,
        entities.Association.entityTypes,
        prov / "agent"   -> agent.asEntityId.asJsonLD,
        prov / "hadPlan" -> association.planId.asEntityId.asJsonLD
      )
    }
  }

  "entityFunctions.findAllPersons" should {

    "return Association's agent if of type Person" in {

      val (association, agent, _) = generateAssociationWithPersonAgent

      EntityFunctions[entities.Association].findAllPersons(association) shouldBe Set(agent.to[entities.Person])
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val (association, _, _) = generateAssociationWithPersonAgent

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Association].encoder(graph)

      association.asJsonLD(functionsEncoder) shouldBe association.asJsonLD
    }
  }

  private def generateAssociationWithPersonAgent: (entities.Association, Person, entities.StepPlan) = {
    val associationWithRenkuAgent =
      activityEntities(stepPlanEntities())(projectCreatedDates().generateOne)
        .map(_.association)
        .generateOne

    val agent = personEntities.generateOne
    val plan  = associationWithRenkuAgent.plan.to[entities.StepPlan]
    val association = entities.Association.WithPersonAgent(
      associationWithRenkuAgent.to[entities.Association].resourceId,
      agent.to[entities.Person],
      plan.resourceId
    )

    (association, agent, plan)
  }

  private def createDecoder(plan: entities.StepPlan): JsonLDDecoder[entities.Association] = {

    implicit val dl: DependencyLinks = (planId: plans.ResourceId) => Option.when(planId == plan.resourceId)(plan)

    entities.Association.decoder
  }
}
