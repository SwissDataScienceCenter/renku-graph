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
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.commandParameters.ParameterDefaultValue
import io.renku.graph.model.testentities.StepPlanCommandParameter.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, plans}
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.syntax._
import monocle.Lens
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ParameterValueSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD VariableParameterValue entity into the VariableParameterValue object " in {
      forAll(nonEmptyStrings().toGeneratorOf(ParameterDefaultValue), parameterValueOverrides) {
        (defaultValue, valueOverride) =>
          val activity =
            executionPlannersDecoupledFromProject(stepPlanEntities(CommandParameter.from(defaultValue))).generateOne
              .planParameterValues(defaultValue -> valueOverride)
              .buildProvenanceUnsafe()
          val entitiesActivity = activity.to[entities.Activity]
          implicit val dl: DependencyLinks = createDependencyLinks(activity.plan.to[entities.StepPlan])

          val Right(parameterValues) = entitiesActivity.asJsonLD.flatten
            .fold(throw _, identity)
            .cursor
            .as(decodeList(entities.ParameterValue.decoder(entitiesActivity.association)))

          parameterValues shouldBe entitiesActivity.parameters
          parameterValues.foreach(_ shouldBe a[entities.ParameterValue.CommandParameterValue])
      }
    }

    "turn JsonLD InputParameterValue entity into the InputParameterValue object " in {
      forAll(entityLocations, entityChecksums) { (location, checksum) =>
        val activity = executionPlannersDecoupledFromProject(
          stepPlanEntities(CommandInput.fromLocation(location))
        ).generateOne
          .planInputParameterValuesFromChecksum(location -> checksum)
          .buildProvenanceUnsafe()
        val entitiesActivity = activity.to[entities.Activity]
        implicit val dl: DependencyLinks = createDependencyLinks(activity.plan.to[entities.StepPlan])

        val Right(parameterValues) = entitiesActivity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as(decodeList(entities.ParameterValue.decoder(entitiesActivity.association)))

        parameterValues shouldBe entitiesActivity.parameters
        parameterValues.foreach(_ shouldBe a[entities.ParameterValue.CommandInputValue])
      }
    }

    "turn JsonLD OutputParameterValue entity into the OutputParameterValue object " in {
      forAll(entityLocations) { location =>
        val activity = executionPlannersDecoupledFromProject(
          stepPlanEntities(CommandOutput.fromLocation(location))
        ).generateOne
          .buildProvenanceUnsafe()
        val entitiesActivity = activity.to[entities.Activity]
        implicit val dl: DependencyLinks = createDependencyLinks(activity.plan.to[entities.StepPlan])

        val Right(parameterValues) = entitiesActivity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as(decodeList(entities.ParameterValue.decoder(entitiesActivity.association)))

        parameterValues shouldBe entitiesActivity.parameters
        parameterValues.foreach(_ shouldBe a[entities.ParameterValue.CommandOutputValue])
      }
    }

    "fail if there are VariableParameterValue for non-existing CommandParameters" in {
      val defaultValue  = nonEmptyStrings().toGeneratorOf(ParameterDefaultValue).generateOne
      val valueOverride = parameterValueOverrides.generateOne
      val activity = executionPlannersDecoupledFromProject(
        stepPlanEntities(CommandParameter.from(defaultValue))
      ).generateOne
        .planParameterValues(defaultValue -> valueOverride)
        .buildProvenanceUnsafe()
      val entitiesActivity = activity.to[entities.Activity]
      implicit val dl: DependencyLinks =
        createDependencyLinks(planParametersLens.modify(_ => Nil)(activity.plan.to[entities.StepPlan]))

      val Left(failure) = entitiesActivity.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.ParameterValue.decoder(entitiesActivity.association)))

      failure shouldBe a[DecodingFailure]
      failure.message should endWith(
        s"ParameterValue points to a non-existing command parameter ${entitiesActivity.parameters.head.valueReference.resourceId}"
      )
    }

    "fail if there are InputParameterValue for non-existing InputParameters" in {
      val location = entityLocations.generateOne
      val checksum = entityChecksums.generateOne
      val activity = executionPlannersDecoupledFromProject(
        stepPlanEntities(CommandInput.fromLocation(location))
      ).generateOne
        .planInputParameterValuesFromChecksum(location -> checksum)
        .buildProvenanceUnsafe()
      val entitiesActivity = activity.to[entities.Activity]
      implicit val dl: DependencyLinks =
        createDependencyLinks(planInputsLens.modify(_ => Nil)(activity.plan.to[entities.StepPlan]))

      val Left(failure) = entitiesActivity.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.ParameterValue.decoder(entitiesActivity.association)))

      failure shouldBe a[DecodingFailure]
      failure.message should endWith(
        s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.StepPlan].inputs.map(_.resourceId).head}"
      )
    }

    "fail if there are OutputParameterValue for non-existing OutputParameters" in {
      val location = entityLocations.generateOne
      val activity = executionPlannersDecoupledFromProject(
        stepPlanEntities(CommandOutput.fromLocation(location))
      ).generateOne
        .buildProvenanceUnsafe()
      val entitiesActivity = activity.to[entities.Activity]
      implicit val dl: DependencyLinks =
        createDependencyLinks(planOutputsLens.modify(_ => Nil)(activity.plan.to[entities.StepPlan]))

      val Left(failure) = entitiesActivity.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as(decodeList(entities.ParameterValue.decoder(entitiesActivity.association)))

      failure shouldBe a[DecodingFailure]
      failure.message should endWith(
        s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.StepPlan].outputs.map(_.resourceId).head}"
      )
    }
  }

  private lazy val planParametersLens
      : Lens[entities.StepPlan, List[entities.StepPlanCommandParameter.CommandParameter]] =
    Lens[entities.StepPlan, List[entities.StepPlanCommandParameter.CommandParameter]](_.parameters) { params =>
      {
        case plan: entities.StepPlan.NonModified => plan.copy(parameters = params)
        case plan: entities.StepPlan.Modified    => plan.copy(parameters = params)
      }
    }

  private lazy val planInputsLens: Lens[entities.StepPlan, List[entities.StepPlanCommandParameter.CommandInput]] =
    Lens[entities.StepPlan, List[entities.StepPlanCommandParameter.CommandInput]](_.inputs) { inputs =>
      {
        case plan: entities.StepPlan.NonModified => plan.copy(inputs = inputs)
        case plan: entities.StepPlan.Modified    => plan.copy(inputs = inputs)
      }
    }

  private lazy val planOutputsLens: Lens[entities.StepPlan, List[entities.StepPlanCommandParameter.CommandOutput]] =
    Lens[entities.StepPlan, List[entities.StepPlanCommandParameter.CommandOutput]](_.outputs) { outputs =>
      {
        case plan: entities.StepPlan.NonModified => plan.copy(outputs = outputs)
        case plan: entities.StepPlan.Modified    => plan.copy(outputs = outputs)
      }
    }

  private def createDependencyLinks(plan: entities.StepPlan) = new DependencyLinks {
    override def findStepPlan(planId: plans.ResourceId) = plan.some
  }
}
