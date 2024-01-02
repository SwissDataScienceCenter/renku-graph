/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.cli.model.{CliCompositePlan, CliPlan, CliStepPlan}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model._
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.ProjectBasedGenFactoryOps
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.jsonld.JsonLDEncoder.encodeEntityId
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PlanSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues
    with AdditionalMatchers
    with DiffInstances {

  "fromCli (StepEntity)" should {

    "turn CliStepPlan of a non-modified Plan entity into the StepPlan object" in {
      forAll(stepPlans) { plan =>
        val prodPlan = plan.to[entities.StepPlan]
        val cliPlan  = plan.to[CliStepPlan]
        entities.StepPlan.fromCli(cliPlan) shouldMatchToValid prodPlan
      }
    }

    "turn CliStepPlan of a modified Plan entity into the StepPlan object" in {
      forAll(stepPlans.map(_.createModification())) { (plan: StepPlan) =>
        val prodPlan = plan.to[entities.StepPlan]
        val cliPlan  = plan.to[CliStepPlan]
        entities.StepPlan.fromCli(cliPlan) shouldMatchToValid prodPlan
      }
    }

    "convert if invalidation after the creation date" in {

      val plan: StepPlan = stepPlans
        .map(_.invalidate())
        .generateOne

      val cliPlan = plan.to[CliPlan]
      entities.Plan.fromCli(cliPlan) shouldMatchToValid plan.to[entities.StepPlan]
    }

    "fail if invalidatedAtTime present on non-modified Plan" in {

      val plan: StepPlan = stepPlans
        .map(_.invalidate())
        .generateOne

      val cliPlan =
        plan
          .to[CliStepPlan]
          .copy(derivedFrom = None)

      val result = entities.StepPlan.fromCli(cliPlan)
      result should beInvalidWithMessageIncluding(
        show"Plan ${plan.to[entities.StepPlan].resourceId} has no parent but invalidation time"
      )
    }

    "fail if invalidation done before the creation date" in {

      val plan: StepPlan = stepPlans
        .map(_.invalidate())
        .generateOne
      val modelPlan = plan.to[entities.StepPlan]

      val wrongInvalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)

      val cliPlan =
        plan
          .to[CliStepPlan]
          .copy(invalidationTime = wrongInvalidationTime.some)

      val result = entities.StepPlan.fromCli(cliPlan)
      result should beInvalidWithMessageIncluding(
        show"Invalidation time $wrongInvalidationTime on StepPlan ${modelPlan.resourceId} is older than dateCreated ${modelPlan.dateCreated}"
      )
    }
  }

  "fromCli (CompositePlan)" should {

    "work for a non-modified composite plan" in {
      forAll(compositePlanGen(cliShapedPersons)) { plan =>
        val expected = plan.to[entities.CompositePlan]
        val cliPlan  = plan.to[CliCompositePlan]

        entities.CompositePlan.fromCli(cliPlan) shouldMatchToValid expected
      }
    }

    "work for a modified composite plan" in {
      forAll(compositePlanGen(cliShapedPersons).map(_.createModification())) { plan =>
        val expected = plan.to[entities.CompositePlan]
        val cliPlan  = plan.to[CliCompositePlan]
        entities.CompositePlan.fromCli(cliPlan) shouldMatchToValid expected
      }
    }

    "work if invalidation after the creation date" in {
      forAll(compositePlanGen(cliShapedPersons).map(_.createModification().invalidate())) { plan =>
        val expected = plan.to[entities.CompositePlan]
        val cliPlan  = plan.to[CliCompositePlan]
        entities.CompositePlan.fromCli(cliPlan) shouldMatchToValid expected
      }
    }

    "work for any Plan entity subtypes" in {
      val testCp = compositePlanGen(cliShapedPersons).generateOne
      val testSp = stepPlans.generateOne
      val cp     = testCp.to[entities.CompositePlan]
      val sp     = testSp.to[entities.StepPlan]

      entities.Plan.fromCli(testCp.to[CliPlan]) shouldMatchToValid cp
      entities.Plan.fromCli(testSp.to[CliPlan]) shouldMatchToValid sp
    }

    "work for a Composite Plan without default value on its Parameter Mapping" in {

      val testPlan                         = compositePlanNonEmptyMappings(cliShapedPersons).generateOne
      val modelPlan                        = testPlan.to[entities.CompositePlan]
      val cliPlan_                         = testPlan.to[CliCompositePlan]
      val cliFirstMapping :: otherMappings = cliPlan_.mappings

      val cliPlan = cliPlan_.copy(mappings = cliFirstMapping.copy(defaultValue = None) :: otherMappings)

      val firstModelMapping = modelPlan.mappings
        .find(_.resourceId == cliFirstMapping.resourceId)
        .getOrElse(fail(s"No Mapping with ${cliFirstMapping.resourceId}"))
      val otherModelMappings = modelPlan.mappings.filterNot(_.resourceId == cliFirstMapping.resourceId)
      val expectedMappings   = firstModelMapping.copy(defaultValue = None) :: otherModelMappings

      entities.CompositePlan.fromCli(cliPlan) shouldMatchToValid modelPlan.fold(
        _.copy(mappings = expectedMappings),
        _.copy(mappings = expectedMappings)
      )
    }

    "fail decode if a parameter maps to itself" in {
      val plan = compositePlanNonEmptyMappings(cliShapedPersons).generateOne
      val pm_  = plan.mappings.head
      val pm   = pm_.copy(mappedParam = NonEmptyList.one(pm_))

      val cliPlan = plan.addParamMapping(pm).to[CliCompositePlan]

      val result = entities.CompositePlan.fromCli(cliPlan)
      result should beInvalidWithMessageIncluding(
        show"Parameter ${pm.to(ParameterMapping.toEntitiesParameterMapping).resourceId} maps to itself"
      )
    }

    "fail if invalidatedAtTime present on non-modified CompositePlan" in {
      val testPlan = compositePlanGen(cliShapedPersons)
        .map(_.createModification().invalidate())
        .generateOne
      val plan = testPlan.to[entities.CompositePlan]
      val cliPlan = testPlan
        .to[CliCompositePlan]
        .copy(derivedFrom = None)

      val result = entities.CompositePlan.fromCli(cliPlan)
      result should beInvalidWithMessageIncluding(
        show"Plan ${plan.resourceId} has no parent but invalidation time"
      )
    }

    "fail if invalidation done before the creation date on a CompositePlan" in {
      val testPlan = compositePlanGen(cliShapedPersons)
        .map(_.createModification().invalidate())
        .generateOne
      val plan = testPlan.to[entities.CompositePlan]

      val invalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)
      val cliPlan = testPlan
        .to[CliCompositePlan]
        .copy(invalidationTime = invalidationTime.some)

      val result = entities.CompositePlan.fromCli(cliPlan)
      result should beInvalidWithMessageIncluding(
        show"Invalidation time $invalidationTime on CompositePlan ${plan.resourceId} is older than dateCreated ${plan.dateCreated}"
      )
    }
  }

  "encode for the Default Graph (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD for a non-modified Plan with all the relevant properties" in {
      val plan = stepPlans.generateOne.replaceCreators(personEntities.generateList(min = 1)).to[entities.StepPlan]

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
      val plan = stepPlans.generateOne
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
      val plan = stepPlans.generateOne.replaceCreators(personEntities.generateList(min = 1)).to[entities.StepPlan]

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
      val plan = stepPlans.generateOne
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
      val plan = stepPlans.generateOne
        .replaceCreators(personEntities.generateList(min = 1))
        .to[entities.StepPlan]

      EntityFunctions[entities.Plan].findAllPersons(plan) shouldBe plan.creators.toSet
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val plan = stepPlans.generateOne.to[entities.Plan]

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Plan].encoder(graph)

      plan.asJsonLD(functionsEncoder) shouldBe plan.asJsonLD
    }
  }

  private lazy val stepPlans: Gen[StepPlan#PlanType] =
    stepPlanEntities(planCommands, cliShapedPersons, commandParametersLists.generateOne: _*)
      .map(_.replaceCreators(cliShapedPersons.generateList(max = 2)))
      .run(projectCreatedDates().generateOne)
}
