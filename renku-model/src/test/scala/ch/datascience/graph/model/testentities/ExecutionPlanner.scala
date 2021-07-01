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

package ch.datascience.graph.model.testentities

import CommandParameterBase.CommandInput._
import CommandParameterBase.CommandOutput._
import CommandParameterBase.CommandParameter.ParameterDefaultValue
import CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import Entity.{Checksum, InputEntity, OutputEntity}
import ExecutionPlanner.ActivityData
import ParameterValue.VariableParameterValue.ValueOverride
import ParameterValue.{PathParameterValue, VariableParameterValue}
import cats.Semigroup
import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.{CliVersion, testentities}

final case class ExecutionPlanner(runPlan:                  RunPlan,
                                  activityData:             ActivityData,
                                  parametersValueOverrides: List[(ParameterDefaultValue, ValueOverride)],
                                  inputsValueOverrides:     List[(InputDefaultValue, Entity)],
                                  outputsValueOverrides:    List[(OutputDefaultValue, Location)]
) {

  def planParameterValues(
      valuesOverrides: (ParameterDefaultValue, ValueOverride)*
  ): ValidatedNel[String, ExecutionPlanner] = {
    val validatedValues = runPlan.parameters.traverse { argument =>
      valuesOverrides
        .foldMapK {
          case (defaultValue, _) => Option.when(defaultValue == argument.defaultValue)(argument)
          case _                 => Option.empty[CommandParameter]
        }
        .toValidNel(s"No execution value for argument with default value ${argument.defaultValue}")
    }

    validatedValues.map(_ => this.copy(parametersValueOverrides = valuesOverrides.toList))
  }

  def planInputParameterValuesFromChecksum(
      valuesOverrides: (Location, Checksum)*
  ): ExecutionPlanner = planInputParameterOverrides(
    valuesOverrides.map { case (location, checksum) =>
      (InputDefaultValue(location), InputEntity(location, checksum))
    }: _*
  )

  def planInputParameterValuesFromEntity(
      valuesOverrides: (Location, OutputEntity)*
  ): ExecutionPlanner =
    this.copy(inputsValueOverrides = inputsValueOverrides ::: valuesOverrides.toList.map { case (location, entity) =>
      (InputDefaultValue(location), entity)
    })

  def planInputParameterOverrides(
      valuesOverrides: (InputDefaultValue, Entity)*
  ): ExecutionPlanner = this.copy(inputsValueOverrides = inputsValueOverrides ::: valuesOverrides.toList)

  def planOutputParameterOverrides(
      valuesOverrides: (OutputDefaultValue, Location)*
  ): ExecutionPlanner = this.copy(outputsValueOverrides = valuesOverrides.toList)

  def buildProvenanceGraph: ValidatedNel[String, Activity] = (
    validateInputsValueOverride,
    validateOutputsValueOverride,
    createParameterFactories,
    validateStartTime
  ) mapN { case (_, _, parameterFactories, (activityTime, author, cliVersion)) =>
    testentities.Activity(
      activityIds.generateOne,
      activityTime,
      Activity.EndTime(activityTime.value),
      author,
      Agent(cliVersion),
      runPlan.project,
      Activity.Order(1),
      Association.factory(Agent(cliVersion), runPlan),
      usageFactories,
      generationFactories,
      parameterFactories
    )
  }

  private lazy val validateInputsValueOverride = runPlan.inputs.traverse {
    case input: LocationCommandInput =>
      inputsValueOverrides
        .foldMapK {
          case (defaultValue, _) => Option.when(defaultValue == input.defaultValue)(input)
          case _                 => Option.empty[CommandInput]
        }
        .toValidNel(
          s"No execution location and checksum for input parameter with default value ${input.defaultValue}"
        )
    case input: MappedCommandInput => Validated.validNel(input)
  }

  private lazy val validateOutputsValueOverride: ValidatedNel[String, List[CommandOutput]] =
    outputsValueOverrides
      .filterNot { case (defaultValue, _) =>
        runPlan.outputs.exists(_.defaultValue == defaultValue)
      } match {
      case Nil => Validated.validNel(runPlan.outputs)
      case invalidOverrides =>
        invalidOverrides.map { case (defaultValue, _) =>
          Validated.invalidNel[String, CommandOutput](s"RunPlan output override defined for non-existing $defaultValue")
        }.sequence
    }

  private lazy val usageFactories = inputsValueOverrides.map { case (_, entity) =>
    Usage.factory(entity)
  }

  private lazy val generationFactories = runPlan.outputs
    .foldLeft(List.empty[LocationCommandOutput]) {
      case (commandOutputs, output: LocationCommandOutput) => commandOutputs :+ output
      case (commandOutputs, _) => commandOutputs
    }
    .map(output =>
      outputsValueOverrides
        .find { case (defaultValue, _) => defaultValue == output.defaultValue }
        .map { case (_, locationOverride) => locationOverride }
        .getOrElse(output.defaultValue.value)
    )
    .map(location => Generation.factory(OutputEntity.factory(location)))

  private lazy val createParameterFactories =
    createVariableParameterFactories |+| createPathParameterFactoriesForInputs |+| createPathParameterFactoriesForOutputs

  private lazy val createVariableParameterFactories = runPlan.parameters.traverse { planParameter =>
    parametersValueOverrides
      .foldMapK {
        case (defaultValue, valueOverride) =>
          Option.when(defaultValue == planParameter.defaultValue)(
            VariableParameterValue.factory(valueOverride, planParameter)
          )
        case _ => Option.empty[Activity => ParameterValue]
      }
      .toValidNel(s"No execution value for parameter with default value ${planParameter.defaultValue}")
  }

  private lazy val createPathParameterFactoriesForInputs = runPlan.inputs.traverse { planInput =>
    inputsValueOverrides
      .foldMapK {
        case (defaultValue, entity) =>
          Option.when(defaultValue == planInput.defaultValue)(
            PathParameterValue.factory(entity.location, planInput)
          )
        case _ => Option.empty[Activity => ParameterValue]
      }
      .toValidNel(s"No execution value for input with default value ${planInput.defaultValue}")
  }

  private lazy val createPathParameterFactoriesForOutputs = runPlan.outputs.traverse { planOutput =>
    val location = outputsValueOverrides
      .find { case (defaultValue, _) => defaultValue == planOutput.defaultValue }
      .map { case (_, locationOverride) => locationOverride }
      .getOrElse(planOutput.defaultValue.value)

    Validated.validNel[String, Activity => ParameterValue] {
      PathParameterValue.factory(location, planOutput)
    }
  }

  private implicit lazy val semigroup: Semigroup[ExecutionPlanner] = (x: ExecutionPlanner, y: ExecutionPlanner) =>
    if (x == y) x else throw new IllegalStateException("Two different ExecutionPlanners cannot be combined")

  private lazy val validateStartTime: ValidatedNel[String, ActivityData] = {
    val (startTime, _, _) = activityData
    Validated.condNel(
      (startTime.value compareTo runPlan.project.dateCreated.value) >= 0,
      activityData,
      s"Activity start time $startTime cannot be older than project ${runPlan.project.dateCreated}"
    )
  }
}

object ExecutionPlanner {

  private type ActivityData = (Activity.StartTime, Person, CliVersion)

  def of(runPlan: RunPlan, activityTime: Activity.StartTime, author: Person, cliVersion: CliVersion): ExecutionPlanner =
    ExecutionPlanner(runPlan, (activityTime, author, cliVersion), List.empty, List.empty, List.empty)
}
