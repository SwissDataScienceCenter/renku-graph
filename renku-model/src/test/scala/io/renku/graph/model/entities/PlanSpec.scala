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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model._
import io.renku.graph.model.cli.CliPlanConverters
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.ProjectBasedGenFactoryOps
import io.renku.graph.model.tools.{AdditionalMatchers, JsonLDTools}
import io.renku.graph.model.tools.JsonLDTools._
import io.renku.jsonld.JsonLDEncoder.encodeEntityId
import io.renku.jsonld._
import io.renku.jsonld.parser._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PlanSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with DiffInstances {

  "decode (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD of a non-modified Plan entity into the StepPlan object" in {
      forAll(plans) { plan =>
        val prodPlan = plan.to[entities.StepPlan]
        CliPlanConverters
          .from(prodPlan)
          .asFlattenedJsonLD
          .cursor
          .as[List[entities.StepPlan]] shouldMatchToRight List(prodPlan)
      }
    }

    "turn JsonLD of a modified Plan entity into the StepPlan object" in {
      forAll(plans.map(_.createModification())) { plan =>
        val cliPlan = CliPlanConverters.from(plan.to[entities.Plan], Nil)
        cliPlan.asFlattenedJsonLD.cursor
          .as[List[entities.StepPlan]] shouldMatchToRight List(plan.to[entities.StepPlan])
      }
    }

    "decode if invalidation after the creation date" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      val cliPlan = CliPlanConverters.from(plan)

      cliPlan.asFlattenedJsonLD.cursor.as[List[entities.StepPlan]] shouldMatchToRight List(plan)
    }

    "fail if invalidatedAtTime present on non-modified Plan" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      val cliPlan =
        CliPlanConverters
          .from(plan)
          .copy(derivedFrom = None)

      val Left(message) = cliPlan.asFlattenedJsonLD.cursor.as[List[entities.StepPlan]].leftMap(_.message)
      message should include(show"Plan ${plan.resourceId} has no parent but modified and invalidation time")
    }

    // This test has been temporarily disabled; see https://github.com/SwissDataScienceCenter/renku-graph/issues/1187
    "fail if invalidation done before the creation date" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      val invalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)
      val jsonLD = parse {
        plan.asJsonLD.toJson.hcursor
          .downField((prov / "invalidatedAtTime").show)
          .delete
          .top
          .getOrElse(fail("Invalid Json after removing property"))
          .deepMerge(
            Json.obj(
              (prov / "invalidatedAtTime").show -> json"""{"@value": ${invalidationTime.show}}"""
            )
          )
      }.flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.StepPlan]].leftMap(_.message)
      message should include {
        show"Invalidation time $invalidationTime on StepPlan ${plan.resourceId} is older than dateCreated ${plan.dateCreated}"
      }
    }
  }

  "decode (CompositePlan)" should {

    "return json for a non-modified composite plan" in {
      forAll(compositePlanGen(cliShapedPersons)) { plan =>
        val expected = plan.to[entities.CompositePlan]
        val allPlans = plan.recursivePlans
        CliPlanConverters
          .from(expected, allPlans.map(_.to[entities.Plan]))
          .asFlattenedJsonLD
          .cursor
          .as[List[entities.CompositePlan]]
          .fold(fail(_), _.filter(_.resourceId == expected.resourceId))
          .shouldMatchTo(List(plan.to[entities.CompositePlan]))
      }
    }

    "return json for a modified composite plan" in {
      forAll(compositePlanGen(cliShapedPersons).map(_.createModification())) { plan =>
        val expected = plan.to[entities.CompositePlan]
        CliPlanConverters
          .from(expected, plan.recursivePlans.map(_.to[entities.Plan]))
          .asFlattenedJsonLD
          .cursor
          .as[List[entities.CompositePlan]]
          .fold(fail(_), _.filter(_.resourceId == expected.resourceId))
          .shouldMatchTo(List(plan.to[entities.CompositePlan]))
      }
    }

    "decode if invalidation after the creation date (composite plan)" in {
      forAll(compositePlanGen(cliShapedPersons).map(_.createModification().invalidate())) { plan =>
        val expected = List(plan.to[entities.CompositePlan])
        val json = CliPlanConverters
          .from(
            plan.to[entities.CompositePlan],
            plan.recursivePlans.map(_.to[entities.Plan])
          )
          .asFlattenedJsonLD
        json.cursor
          .as[List[entities.CompositePlan]]
          .fold(fail(_), _.filter(_.resourceId == expected.head.resourceId))
          .shouldMatchTo(expected)
      }
    }

    "decode any Plan entity subtypes" in {
      val testCp = compositePlanGen(cliShapedPersons).generateOne
      val testSp = plans.generateOne
      val cp     = testCp.to[entities.CompositePlan]
      val sp     = testSp.to[entities.StepPlan]

      val jsonLD = flattenedJsonLDFrom(
        CliPlanConverters
          .from(cp, testCp.recursivePlans.map(_.to[entities.Plan]))
          .asNestedJsonLD,
        CliPlanConverters.from(sp).asNestedJsonLD
      )

      val decoded = jsonLD.cursor
        .as[List[entities.Plan]]
        .map(_.filter(p => Set(cp.resourceId, sp.resourceId).contains(p.resourceId)))
        .map(_.sortBy(_.resourceId.value))

      decoded shouldMatchToRight List(sp, cp).sortBy(_.resourceId.value)
    }

    "decode a Composite Plan that has additional types" in {
      val testPlan = compositePlanGen(cliShapedPersons).generateOne
      val plan     = testPlan.to[entities.CompositePlan]
      val cliPlan  = CliPlanConverters.from(plan, testPlan.recursivePlans.map(_.to[entities.Plan]))

      val newJson =
        JsonLDTools
          .view(cliPlan)
          .selectByTypes(entities.CompositePlan.Ontology.entityTypes)
          .addType(renku / "WorkflowCompositePlan")
          .value

      newJson.cursor
        .as[List[entities.CompositePlan]]
        .map(_.filter(_.resourceId == plan.resourceId)) shouldMatchToRight List(plan)
    }

    "decode a Step Plan that has additional types" in {
      val testPlan = plans.generateOne
      val plan     = testPlan.to[entities.StepPlan]
      val cliPlan  = CliPlanConverters.from(plan)

      val newJson =
        JsonLDTools
          .view(cliPlan)
          .selectByTypes(entities.StepPlan.entityTypes)
          .addType(renku / "WorkflowPlan")
          .value

      newJson.cursor.as[List[entities.StepPlan]] shouldBe List(plan).asRight
    }

    "fail decode if a parameter maps to itself" in {
      val plan = compositePlanNonEmptyMappings(cliShapedPersons).generateOne
      val pm_  = plan.mappings.head
      val pm   = pm_.copy(mappedParam = NonEmptyList.one(pm_))

      val ex = intercept[Throwable] {
        plan
          .addParamMapping(pm)
          .to[entities.CompositePlan]
      }

      ex.getMessage should include(
        show"Parameter ${pm.to(ParameterMapping.toEntitiesParameterMapping).resourceId} maps to itself"
      )
    }

    "fail if invalidatedAtTime present on non-modified CompositePlan" in {
      val testPlan = compositePlanGen(cliShapedPersons)
        .map(_.createModification().invalidate())
        .generateOne
      val plan = testPlan.to[entities.CompositePlan]
      val cliPlan = CliPlanConverters
        .from(plan, testPlan.recursivePlans.map(_.to[entities.Plan]))
        .copy(derivedFrom = None)

      val jsonLD = cliPlan.asFlattenedJsonLD

      val Left(message) = jsonLD.cursor.as[List[entities.CompositePlan]].leftMap(_.message)
      message should include(show"Plan ${plan.resourceId} has no parent but invalidation time")
    }

    "fail if invalidation done before the creation date on a CompositePlan" in {
      val testPlan = compositePlanGen(cliShapedPersons)
        .map(_.createModification().invalidate())
        .generateOne
      val plan = testPlan.to[entities.CompositePlan]

      val invalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)
      val cliPlan = CliPlanConverters
        .from(plan, testPlan.recursivePlans.map(_.to[entities.Plan]))
        .copy(invalidationTime = invalidationTime.some)

      val jsonLD = cliPlan.asNestedJsonLD

      val Left(message) = jsonLD.cursor.as[List[entities.CompositePlan]].leftMap(_.message)
      message should include {
        show"Invalidation time $invalidationTime on CompositePlan ${plan.resourceId} is older than dateCreated ${plan.dateCreated}"
      }
    }
  }

  "encode for the Default Graph (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD for a non-modified Plan with all the relevant properties" in {
      val plan = plans.generateOne.replaceCreators(personEntities.generateList(min = 1)).to[entities.StepPlan]

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.resourceId.asEntityId.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD
        )
    }

    "produce JsonLD for a modified Plan with all the relevant properties" in {
      val plan = plans.generateOne
        .invalidate()
        .replaceCreators(personEntities.generateList(min = 1))
        .to(StepPlan.Modified.toEntitiesStepPlan)

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD,
          prov / "wasDerivedFrom"        -> plan.derivation.derivedFrom.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.derivation.originalResourceId.asEntityId.asJsonLD,
          prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
        )
    }
  }

  "encode for the Named Graphs (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD for a non-modified Plan with all the relevant properties" in {
      val plan = plans.generateOne.replaceCreators(personEntities.generateList(min = 1)).to[entities.StepPlan]

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.resourceId.asEntityId.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD
        )
    }

    "produce JsonLD for a modified Plan with all the relevant properties" in {
      val plan = plans.generateOne
        .invalidate()
        .replaceCreators(personEntities.generateList(min = 1))
        .to(StepPlan.Modified.toEntitiesStepPlan)

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD,
          prov / "wasDerivedFrom"        -> plan.derivation.derivedFrom.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.derivation.originalResourceId.asEntityId.asJsonLD,
          prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
        )
    }
  }
  "encode for the Named Graphs (CompositePlan)" should {
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD for a non-modified composite plan" in {
      val plan: entities.CompositePlan =
        compositePlanGen(cliShapedPersons).generateOne.to[entities.CompositePlan]

      plan.asJsonLD shouldBe JsonLD.entity(
        plan.resourceId.asEntityId,
        entities.CompositePlan.Ontology.entityTypes,
        schema / "name"              -> plan.name.asJsonLD,
        schema / "description"       -> plan.maybeDescription.asJsonLD,
        schema / "creator"           -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
        schema / "dateCreated"       -> plan.dateCreated.asJsonLD,
        schema / "keywords"          -> plan.keywords.asJsonLD,
        renku / "hasSubprocess"      -> plan.plans.toList.asJsonLD,
        renku / "workflowLinks"      -> plan.links.asJsonLD,
        renku / "hasMappings"        -> plan.mappings.asJsonLD,
        renku / "topmostDerivedFrom" -> plan.resourceId.asEntityId.asJsonLD
      )
    }

    "produce JsonLD for a modified composite plan" in {
      val plan =
        compositePlanGen(cliShapedPersons).generateOne
          .createModification(identity)
          .invalidate()
          .to(CompositePlan.Modified.toEntitiesCompositePlan)

      (plan: entities.CompositePlan).asJsonLD shouldBe JsonLD.entity(
        plan.resourceId.asEntityId,
        entities.CompositePlan.Ontology.entityTypes,
        schema / "name"              -> plan.name.asJsonLD,
        schema / "description"       -> plan.maybeDescription.asJsonLD,
        schema / "creator"           -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
        schema / "dateCreated"       -> plan.dateCreated.asJsonLD,
        schema / "keywords"          -> plan.keywords.asJsonLD,
        renku / "hasSubprocess"      -> plan.plans.toList.asJsonLD,
        renku / "workflowLinks"      -> plan.links.asJsonLD,
        renku / "hasMappings"        -> plan.mappings.asJsonLD,
        renku / "topmostDerivedFrom" -> plan.resourceId.asEntityId.asJsonLD,
        prov / "invalidatedAtTime"   -> plan.maybeInvalidationTime.asJsonLD,
        prov / "wasDerivedFrom"      -> plan.derivation.derivedFrom.asJsonLD,
        renku / "topmostDerivedFrom" -> plan.derivation.originalResourceId.asEntityId.asJsonLD
      )
    }
  }

  "entityFunctions.findAllPersons" should {

    "return all creators" in {
      val plan = plans.generateOne
        .replaceCreators(personEntities.generateList(min = 1))
        .to[entities.StepPlan]

      EntityFunctions[entities.Plan].findAllPersons(plan) shouldBe plan.creators.toSet
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val plan = plans.generateOne.to[entities.Plan]

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Plan].encoder(graph)

      plan.asJsonLD(functionsEncoder) shouldBe plan.asJsonLD
    }
  }

  private lazy val plans =
    stepPlanEntities(planCommands, cliShapedPersons, commandParametersLists.generateOne: _*)
      .map(_.replaceCreators(cliShapedPersons.generateList(max = 2)))
      .run(projectCreatedDates().generateOne)

  private def flattenedJsonLD[A: JsonLDEncoder](value: A): JsonLD =
    JsonLDTools.flattenedJsonLD(value)
}
