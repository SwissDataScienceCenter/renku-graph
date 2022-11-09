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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model._
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import io.renku.jsonld.JsonLDEncoder.encodeEntityId
import io.renku.jsonld.parser._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PlanSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD of a non-modified Plan entity into the StepPlan object" in {
      forAll(plans) { plan =>
        flattenedJsonLD(plan).cursor
          .as[List[entities.StepPlan]] shouldBe List(plan.to[entities.StepPlan]).asRight
      }
    }

    "turn JsonLD of a modified Plan entity into the StepPlan object" in {
      forAll(plans.map(_.createModification())) { plan =>
        flattenedJsonLD(plan).cursor
          .as[List[entities.StepPlan]] shouldBe List(plan.to[entities.StepPlan]).asRight
      }
    }

    "decode if invalidation after the creation date" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      flattenedJsonLD(plan).cursor.as[List[entities.StepPlan]] shouldBe List(plan).asRight
    }

    "fail if invalidatedAtTime present on non-modified Plan" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      val jsonLD = parse {
        plan.asJsonLD.toJson.hcursor
          .downField((prov / "wasDerivedFrom").show)
          .delete
          .top
          .getOrElse(fail("Invalid Json after removing property"))
      }.flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.StepPlan]].leftMap(_.message)
      message should include(show"Plan ${plan.resourceId} has no parent but invalidation time")
    }

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

  "decode (CompositeEntity)" should {
    implicit val graphClass: GraphClass = GraphClass.Default

    "return json for a non-modified composite plan" in {
      forAll(compositePlanEntities()(projectCreatedDates().generateOne)) { plan =>
        flattenedJsonLD(plan).cursor
          .as[List[entities.CompositePlan]] shouldBe List(plan.to[entities.CompositePlan]).asRight
      }
    }

    "return json for a modified composite plan" in {
      forAll(compositePlanEntities()(projectCreatedDates().generateOne).map(_.createModification())) { plan =>
        flattenedJsonLD(plan).cursor
          .as[List[entities.CompositePlan]] shouldBe List(plan.to[entities.CompositePlan]).asRight
      }
    }

    "decode if invalidation after the creation date (composite plan)" in {
      val plan = compositePlanEntities()(projectCreatedDates().generateOne)
        .map(_.createModification().invalidate())
        .generateOne
        .to[entities.CompositePlan]

      flattenedJsonLD(plan).cursor.as[List[entities.CompositePlan]] shouldBe List(plan).asRight
    }

    "pick up the correct decoder" in {
      val cp = compositePlanEntities()(projectCreatedDates().generateOne).generateOne
        .to[entities.CompositePlan]

      val sp = plans.generateOne.to[entities.StepPlan]

      val spDecoded = flattenedJsonLD(sp).cursor.as[List[entities.Plan]]
      val cpDecoded = flattenedJsonLD(cp).cursor.as[List[entities.Plan]]

      spDecoded shouldBe Right(List(sp))
      cpDecoded shouldBe Right(List(cp))
    }

    "fail decode if a parameter maps to itself" in {
      val pm_ = parameterMappingEntities.generateOne
      val pm  = pm_.copy(mappedParameter = NonEmptyList.one(pm_.id.value))

      val cp = compositePlanEntities()(projectCreatedDates().generateOne).generateOne
        .asInstanceOf[CompositePlan.NonModified]
        .addParamMapping(pm)
        .to[entities.CompositePlan]

      val decoded = flattenedJsonLD(cp).cursor
        .as[List[entities.CompositePlan]]
        .swap
        .getOrElse(sys.error("Expected decoding to fail, but it was successful"))

      decoded.message should include(
        show"Parameter ${pm.toEntitiesParameterMapping.resourceId} maps to itself"
      )
    }

    "fail if invalidatedAtTime present on non-modified CompositePlan" in {
      val plan = compositePlanEntities()(projectCreatedDates().generateOne)
        .map(_.createModification().invalidate())
        .generateOne
        .to[entities.CompositePlan]
      val jsonValue = plan.asJsonLD.toJson.hcursor
        .downField((prov / "wasDerivedFrom").show)
        .delete
        .top
        .getOrElse(fail("Invalid Json after removing property"))

      val jsonLD = parse(jsonValue).flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.CompositePlan]].leftMap(_.message)
      message should include(show"Plan ${plan.resourceId} has no parent but invalidation time")
    }

    "fail if invalidation done before the creation date on a CompositePlan" in {
      val plan = compositePlanEntities()(projectCreatedDates().generateOne)
        .map(_.createModification().invalidate())
        .generateOne
        .to[entities.CompositePlan]

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
        compositePlanEntities()(projectCreatedDates().generateOne).generateOne
          .to[entities.CompositePlan]

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
        compositePlanEntities()(projectCreatedDates().generateOne).generateOne
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

  private lazy val plans = stepPlanEntities(commandParametersLists.generateOne: _*)(planCommands)
    .modify(_.replaceCreators(personEntities.generateList(max = 2)))(projectCreatedDates().generateOne)

  private def flattenedJsonLD[A: JsonLDEncoder](value: A): JsonLD =
    JsonLDEncoder[A].apply(value).flatten.fold(fail(_), identity)
}
