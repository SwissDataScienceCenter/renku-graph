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
import io.renku.cli.model.CliAssociation
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, plans}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDDecoder}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AssociationSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks with EitherValues {

  "decode" should {

    "turn JsonLD Association entity with Renku agent into the Association object" in {
      forAll(
        activityEntities(stepPlanEntities(planCommands, cliShapedPersons), cliShapedPersons)(
          projectCreatedDates().generateOne
        ).map(_.association)
      ) { association =>
        implicit val decoder: JsonLDDecoder[entities.Association] =
          createDecoder(association.plan.to[entities.StepPlan])

        association.to[CliAssociation].asFlattenedJsonLD.cursor.as[List[entities.Association]] shouldBe
          List(association.to[entities.Association]).asRight
      }
    }

    "turn JsonLD Association entity with Person agent into the Association object" in {

      val (association, _, plan) = generateAssociationWithPersonAgent

      implicit val decoder: JsonLDDecoder[entities.Association] = createDecoder(plan.to[entities.StepPlan])

      association
        .to[CliAssociation]
        .asFlattenedJsonLD
        .cursor
        .as[List[entities.Association]] shouldBe List(association.to[entities.Association]).asRight
    }

    "fail decoding if there's no Plan with the Id the Association points to" in {

      val testAssociation =
        activityEntities(stepPlanEntities(planCommands, cliShapedPersons), cliShapedPersons)(
          projectCreatedDates().generateOne
        )
          .map(_.association)
          .generateOne

      val association = testAssociation.to[entities.Association]

      implicit val dl: DependencyLinks = (_: plans.ResourceId) => Option.empty[entities.StepPlan]

      val result = testAssociation.to[CliAssociation].asFlattenedJsonLD.cursor.as[List[entities.Association]]

      result.left.value.message should include(
        show"Association ${association.resourceId} points to a non-existing Plan ${association.planId}"
      )
    }
  }

  "encode for the Default Graph" should {
    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD with all the relevant properties" in {

      val (association, agent) = generateEntitiesAssociationWithPersonAgent

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

      val (association, agent) = generateEntitiesAssociationWithPersonAgent

      association.asJsonLD shouldBe JsonLD.entity(
        association.resourceId.asEntityId,
        entities.Association.entityTypes,
        prov / "agent"   -> agent.resourceId.asEntityId.asJsonLD,
        prov / "hadPlan" -> association.planId.asEntityId.asJsonLD
      )
    }
  }

  "entityFunctions.findAllPersons" should {

    "return Association's agent if of type Person" in {

      val (association, agent) = generateEntitiesAssociationWithPersonAgent

      EntityFunctions[entities.Association].findAllPersons(association) shouldBe Set(agent)
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val (association, _) = generateEntitiesAssociationWithPersonAgent

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Association].encoder(graph)

      association.asJsonLD(functionsEncoder) shouldBe association.asJsonLD
    }
  }

  private def generateEntitiesAssociationWithPersonAgent: (entities.Association, entities.Person) = {
    val (association, agent, _) = generateAssociationWithPersonAgent
    (association.to[entities.Association], agent.to[entities.Person])
  }

  private def generateAssociationWithPersonAgent: (Association, Person, StepPlan) = {
    val associationWithRenkuAgent =
      activityEntities(stepPlanEntities(planCommands, cliShapedPersons), cliShapedPersons)(
        projectCreatedDates().generateOne
      )
        .map(_.association)
        .generateOne

    val agent = cliShapedPersons.generateOne
    val plan  = associationWithRenkuAgent.plan
    val association = Association.WithPersonAgent(
      associationWithRenkuAgent.activity,
      agent,
      associationWithRenkuAgent.plan
    )

    (association, agent, plan)
  }

  private def createDecoder(plan: entities.StepPlan): JsonLDDecoder[entities.Association] = {

    implicit val dl: DependencyLinks = (planId: plans.ResourceId) => Option.when(planId == plan.resourceId)(plan)

    entities.Association.decoder
  }
}
