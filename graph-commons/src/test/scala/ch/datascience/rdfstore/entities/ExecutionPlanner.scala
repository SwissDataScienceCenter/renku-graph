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

package ch.datascience.rdfstore.entities

import cats.Semigroup
import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.CliVersion
import ch.datascience.rdfstore.entities.CommandParameterBase.CommandParameter.ParameterDefaultValue
import ch.datascience.rdfstore.entities.CommandParameterBase.CommandInput._
import ch.datascience.rdfstore.entities.CommandParameterBase.CommandOutput.LocationCommandOutput
import ch.datascience.rdfstore.entities.CommandParameterBase.{CommandInput, CommandParameter}
import ch.datascience.rdfstore.entities.Entity.{Checksum, InputEntity, OutputEntity}
import ch.datascience.rdfstore.entities.ParameterValue.VariableParameterValue.ValueOverride
import ch.datascience.rdfstore.entities.ParameterValue.{PathParameterValue, VariableParameterValue}

final case class ExecutionPlanner(runPlan:                 RunPlan,
                                  activityData:            (Activity.StartTime, Person, CliVersion),
                                  project:                 Project[Project.ForksCount],
                                  argumentsValueOverrides: List[(ParameterDefaultValue, ValueOverride)],
                                  inputsValueOverrides:    List[(InputDefaultValue, Location, Checksum)]
) {

  def planParameterArgumentsValues(
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

    validatedValues.map(_ => this.copy(argumentsValueOverrides = valuesOverrides.toList))
  }

  def planParameterInputsValues(
      valuesOverrides: (Location, Checksum)*
  ): ExecutionPlanner = planParameterInputsOverrides(
    valuesOverrides.map { case (location, checksum) =>
      (InputDefaultValue(location), location, checksum)
    }: _*
  )

  def planParameterInputsOverrides(
      valuesOverrides: (InputDefaultValue, Location, Checksum)*
  ): ExecutionPlanner = this.copy(inputsValueOverrides = valuesOverrides.toList)

  def buildProvenanceGraph: ValidatedNel[String, Activity] = (
    validateInputsValueOverride,
    createParameterFactories,
    activityData.validNel[String]
  ) mapN { case (_, parameterFactories, (activityTime, author, cliVersion)) =>
    Activity(
      activityIds.generateOne,
      activityTime,
      Activity.EndTime(activityTime.value),
      author,
      Agent(cliVersion),
      project,
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
          case (defaultValue, _, _) => Option.when(defaultValue == input.defaultValue)(input)
          case _                    => Option.empty[CommandInput]
        }
        .toValidNel(
          s"No execution location and checksum for input parameter with default value ${input.defaultValue}"
        )
    case input: MappedCommandInput => Validated.validNel(input)
  }

  private lazy val usageFactories = inputsValueOverrides.map { case (_, location, checksum) =>
    Usage.factory(InputEntity(location, checksum))
  }

  private lazy val generationFactories = runPlan.outputs
    .foldLeft(List.empty[LocationCommandOutput]) {
      case (commandOutputs, output: LocationCommandOutput) => commandOutputs :+ output
      case (commandOutputs, _) => commandOutputs
    }
    .map(commandOutput => Generation.factory(OutputEntity.factory(commandOutput.defaultValue.asLocation)))

  private lazy val createParameterFactories = createVariableParameterFactories map {
    _ :++ runPlan.inputs.map(PathParameterValue.factory) :++ runPlan.outputs.map(PathParameterValue.factory)
  }

  private lazy val createVariableParameterFactories = runPlan.parameters.traverse { planArgument =>
    argumentsValueOverrides
      .foldMapK {
        case (defaultValue, valueOverride) =>
          Option.when(defaultValue == planArgument.defaultValue)(
            VariableParameterValue.factory(valueOverride, planArgument)
          )
        case _ => Option.empty[Activity => ParameterValue]
      }
      .toValidNel(s"No execution value for argument with default value ${planArgument.defaultValue}")
  }

  private implicit lazy val semigroup: Semigroup[ExecutionPlanner] = (x: ExecutionPlanner, y: ExecutionPlanner) =>
    if (x == y) x else throw new IllegalStateException("Two different ExecutionPlanners cannot be combined")
}

object ExecutionPlanner {
  def of(runPlan:      RunPlan,
         activityTime: Activity.StartTime,
         author:       Person,
         cliVersion:   CliVersion,
         project:      Project[Project.ForksCount]
  ): ExecutionPlanner = ExecutionPlanner(runPlan, (activityTime, author, cliVersion), project, List.empty, List.empty)
}
