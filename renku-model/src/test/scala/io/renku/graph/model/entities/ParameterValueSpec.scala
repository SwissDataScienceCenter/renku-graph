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

import cats.syntax.all._
import io.renku.cli.model.CliParameterValue
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.commandParameters.ParameterDefaultValue
import io.renku.graph.model.testentities.StepPlan.CommandParameters.CommandParameterFactory
import io.renku.graph.model.testentities.StepPlanCommandParameter.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.testentities._
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.entities
import org.scalacheck.Gen
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ParameterValueSpec
    extends AnyWordSpec
    with should.Matchers
    with EitherValues
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with DiffInstances {

  def executionPlannerGen(commandParam: CommandParameterFactory): Gen[ExecutionPlanner] =
    executionPlanners(
      stepPlanEntities(planCommands, cliShapedPersons, commandParam),
      projectCreatedDates().generateOne,
      cliShapedPersons
    )

  "fromCli" should {
    "turn cli VariableParameterValue entity into the VariableParameterValue object " in {
      forAll(nonEmptyStrings().toGeneratorOf(ParameterDefaultValue), parameterValueOverrides) {
        (defaultValue, valueOverride) =>
          val activity =
            executionPlannerGen(CommandParameter.from(defaultValue)).generateOne
              .planParameterValues(defaultValue -> valueOverride)
              .buildProvenanceUnsafe()
          val entitiesActivity = activity.to[entities.Activity]

          val result =
            activity.parameters.traverse(p =>
              entities.ParameterValue.fromCli(p.to[CliParameterValue], activity.plan.to[entities.StepPlan])
            )

          result shouldMatchToValid entitiesActivity.parameters
          entitiesActivity.parameters.foreach(_ shouldBe a[entities.ParameterValue.CommandParameterValue])
      }
    }

    "turn cli InputParameterValue entity into the InputParameterValue object " in {
      forAll(entityLocations, entityChecksums) { (location, checksum) =>
        val activity = executionPlannerGen(CommandInput.fromLocation(location)).generateOne
          .planInputParameterValuesFromChecksum(location -> checksum)
          .buildProvenanceUnsafe()
        val entitiesActivity = activity.to[entities.Activity]

        val result =
          activity.parameters.traverse(p =>
            entities.ParameterValue.fromCli(p.to[CliParameterValue], activity.plan.to[entities.StepPlan])
          )

        result shouldMatchToValid entitiesActivity.parameters
        entitiesActivity.parameters.foreach(_ shouldBe a[entities.ParameterValue.CommandInputValue])
      }
    }

    "turn JsonLD OutputParameterValue entity into the OutputParameterValue object " in {
      forAll(entityLocations) { location =>
        val activity = executionPlannerGen(CommandOutput.fromLocation(location)).generateOne
          .buildProvenanceUnsafe()
        val entitiesActivity = activity.to[entities.Activity]

        val result =
          activity.parameters.traverse(p =>
            entities.ParameterValue.fromCli(p.to[CliParameterValue], activity.plan.to[entities.StepPlan])
          )

        result shouldMatchToValid entitiesActivity.parameters
        entitiesActivity.parameters.foreach(_ shouldBe a[entities.ParameterValue.CommandOutputValue])
      }
    }

    "fail if there are VariableParameterValue for non-existing CommandParameters" in {
      val defaultValue  = nonEmptyStrings().toGeneratorOf(ParameterDefaultValue).generateOne
      val valueOverride = parameterValueOverrides.generateOne
      val activity = executionPlannerGen(CommandParameter.from(defaultValue)).generateOne
        .planParameterValues(defaultValue -> valueOverride)
        .buildProvenanceUnsafe()
      val entitiesActivity = activity.to[entities.Activity]
      val unrelatedPlan    = stepPlanEntities().apply(projectCreatedDates().generateOne).generateOne

      val result =
        activity.parameters.traverse(p =>
          entities.ParameterValue.fromCli(p.to[CliParameterValue], unrelatedPlan.to[entities.StepPlan])
        )

      result should beInvalidWithMessageIncluding(
        s"ParameterValue points to a non-existing command parameter ${entitiesActivity.parameters.head.valueReference.resourceId}"
      )
    }

    "fail if there are InputParameterValue for non-existing InputParameters" in {
      val location = entityLocations.generateOne
      val checksum = entityChecksums.generateOne
      val activity = executionPlannerGen(CommandInput.fromLocation(location)).generateOne
        .planInputParameterValuesFromChecksum(location -> checksum)
        .buildProvenanceUnsafe()
      val unrelatedPlan = stepPlanEntities().apply(projectCreatedDates().generateOne).generateOne

      val result =
        activity.parameters.traverse(p =>
          entities.ParameterValue.fromCli(p.to[CliParameterValue], unrelatedPlan.to[entities.StepPlan])
        )

      result should beInvalidWithMessageIncluding(
        s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.StepPlan].inputs.map(_.resourceId).head}"
      )
    }

    "fail if there are OutputParameterValue for non-existing OutputParameters" in {
      val location = entityLocations.generateOne
      val activity = executionPlannerGen(CommandOutput.fromLocation(location)).generateOne
        .buildProvenanceUnsafe()
      val unrelatedPlan = stepPlanEntities().apply(projectCreatedDates().generateOne).generateOne

      val result =
        activity.parameters.traverse(p =>
          entities.ParameterValue.fromCli(p.to[CliParameterValue], unrelatedPlan.to[entities.StepPlan])
        )

      result should beInvalidWithMessageIncluding(
        s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.StepPlan].outputs.map(_.resourceId).head}"
      )
    }
  }
}
