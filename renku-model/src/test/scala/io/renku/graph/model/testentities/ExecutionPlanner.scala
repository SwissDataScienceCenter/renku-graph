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

package io.renku.graph.model.testentities

import cats.Semigroup
import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.commandParameters._
import io.renku.graph.model.entityModel._
import io.renku.graph.model.parameterValues._
import io.renku.graph.model.testentities.CommandParameterBase.CommandInput._
import io.renku.graph.model.testentities.CommandParameterBase.CommandOutput._
import io.renku.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.testentities.Entity.{InputEntity, OutputEntity}
import io.renku.graph.model.testentities.ExecutionPlanner.ActivityData
import io.renku.graph.model.testentities.ParameterValue.{CommandParameterValue, LocationParameterValue}

final case class ExecutionPlanner(plan:                     Plan,
                                  activityData:             ActivityData,
                                  parametersValueOverrides: List[(ParameterDefaultValue, ValueOverride)],
                                  inputsValueOverrides:     List[(InputDefaultValue, Entity)],
                                  outputsValueOverrides:    List[(OutputDefaultValue, Location)],
                                  projectDateCreated:       projects.DateCreated
) {

  def planParameterValues(
      valuesOverrides: (ParameterDefaultValue, ValueOverride)*
  ): ExecutionPlanner = this.copy(parametersValueOverrides = parametersValueOverrides ::: valuesOverrides.toList)

  def planInputParameterValuesFromChecksum(
      valuesOverrides: (Location, Checksum)*
  ): ExecutionPlanner = planInputParameterOverrides(
    valuesOverrides.map { case (location, checksum) => location -> InputEntity(location, checksum) }: _*
  )

  def planInputParameterValuesFromEntity(
      valuesOverrides: (Location, OutputEntity)*
  ): ExecutionPlanner =
    this.copy(inputsValueOverrides = inputsValueOverrides ::: valuesOverrides.toList.map { case (location, entity) =>
      (InputDefaultValue(location), entity)
    })

  def planInputParameterOverrides(
      valuesOverrides: (Location, Entity)*
  ): ExecutionPlanner = this.copy(
    inputsValueOverrides = inputsValueOverrides ::: valuesOverrides.map { case (planInputLocation, overrideEntity) =>
      InputDefaultValue(planInputLocation) -> overrideEntity
    }.toList
  )

  def planOutputParameterOverrides(
      valuesOverrides: (Location, Location)*
  ): ExecutionPlanner = this.copy(
    outputsValueOverrides = valuesOverrides.map { case (planOutputLocation, overrideLocation) =>
      OutputDefaultValue(planOutputLocation) -> overrideLocation
    }.toList
  )

  def buildProvenanceGraph: ValidatedNel[String, Activity] = (
    validateParameterValueOverride,
    validateInputsValueOverride,
    validateOutputsValueOverride,
    createParameterFactories,
    createGenerationFactories,
    validateStartTime
  ) mapN { case (_, _, _, parameterFactories, generationFactories, (activityTime, author, cliVersion)) =>
    testentities.Activity(
      activityIds.generateOne,
      activityTime,
      activities.EndTime(activityTime.value),
      author,
      Agent(cliVersion),
      Association.factory(Agent(cliVersion), plan),
      usageFactories,
      generationFactories,
      parameterFactories
    )
  }

  def buildProvenanceUnsafe(): Activity =
    buildProvenanceGraph.fold(errors => throw new Exception(errors.intercalate("\n")), identity)

  private lazy val validateParameterValueOverride = plan.parameters.traverse { parameter =>
    parametersValueOverrides
      .foldMapK {
        case (defaultValue, _) => Option.when(defaultValue == parameter.defaultValue)(parameter)
        case _                 => Option.empty[CommandParameter]
      }
      .toValidNel(s"No execution value for parameter with default value ${parameter.defaultValue}")
  }

  private lazy val validateInputsValueOverride = plan.inputs.traverse {
    case input: LocationCommandInput => validateChecksumForInputs(input)
    case input: MappedCommandInput   => Validated.validNel(input)
    case input: ImplicitCommandInput => validateChecksumForInputs(input)
  }

  private def validateChecksumForInputs(input: CommandInput) =
    inputsValueOverrides
      .foldMapK {
        case (defaultValue, _) => Option.when(defaultValue == input.defaultValue)(input)
        case _                 => Option.empty[CommandInput]
      }
      .toValidNel(
        s"No execution location and checksum for input parameter with default value ${input.defaultValue}"
      )

  private lazy val validateOutputsValueOverride: ValidatedNel[String, List[CommandOutput]] =
    outputsValueOverrides
      .filterNot { case (defaultValue, _) =>
        plan.outputs.exists(_.defaultValue == defaultValue)
      } match {
      case Nil => Validated.validNel(plan.outputs)
      case invalidOverrides =>
        invalidOverrides.map { case (defaultValue, _) =>
          Validated.invalidNel[String, CommandOutput](s"Plan output override defined for non-existing $defaultValue")
        }.sequence
    }

  private lazy val usageFactories = inputsValueOverrides.map { case (_, entity) =>
    Usage.factory(entity)
  }

  private lazy val createGenerationFactories: ValidatedNel[String, List[Activity => Generation]] = plan.outputs
    .foldLeft(List.empty[LocationCommandOutput]) {
      case (commandOutputs, output: LocationCommandOutput) => commandOutputs :+ output
      case (commandOutputs, _)                             => commandOutputs
    }
    .map(output =>
      outputsValueOverrides
        .find { case (defaultValue, _) => defaultValue == output.defaultValue }
        .map { case (_, locationOverride) => locationOverride }
        .getOrElse(output.defaultValue.value)
    )
    .map {
      case _: Location.FileOrFolder =>
        "FileOrFolder Location cannot be used to define output default value".invalidNel[Location]
      case location: Location => location.validNel[String]
    }
    .sequence
    .map(locations => locations.map(location => Generation.factory(OutputEntity.factory(location))))

  private lazy val createParameterFactories =
    createVariableParameterFactories |+| createPathParameterFactoriesForInputs |+| createPathParameterFactoriesForOutputs

  private lazy val createVariableParameterFactories = plan.parameters.traverse { planParameter =>
    parametersValueOverrides
      .foldMapK {
        case (defaultValue, valueOverride) =>
          Option.when(defaultValue == planParameter.defaultValue)(
            CommandParameterValue.factory(valueOverride, planParameter)
          )
        case _ => Option.empty[Activity => ParameterValue]
      }
      .toValidNel(s"No execution value for parameter with default value ${planParameter.defaultValue}")
  }

  private lazy val createPathParameterFactoriesForInputs = plan.inputs.traverse { planInput =>
    inputsValueOverrides
      .foldMapK {
        case (defaultValue, entity) =>
          Option.when(defaultValue == planInput.defaultValue)(
            LocationParameterValue.factory(entity.location, planInput)
          )
        case _ => Option.empty[Activity => ParameterValue]
      }
      .toValidNel(s"No execution value for input with default value ${planInput.defaultValue}")
  }

  private lazy val createPathParameterFactoriesForOutputs = plan.outputs.traverse { planOutput =>
    val location = outputsValueOverrides
      .find { case (defaultValue, _) => defaultValue == planOutput.defaultValue }
      .map { case (_, locationOverride) => locationOverride }
      .getOrElse(planOutput.defaultValue.value)

    Validated.validNel[String, Activity => ParameterValue] {
      LocationParameterValue.factory(location, planOutput)
    }
  }

  private implicit lazy val semigroup: Semigroup[ExecutionPlanner] = (x: ExecutionPlanner, y: ExecutionPlanner) =>
    if (x == y) x else throw new IllegalStateException("Two different ExecutionPlanners cannot be combined")

  private lazy val validateStartTime: ValidatedNel[String, ActivityData] = {
    val (startTime, _, _) = activityData
    Validated.condNel(
      (startTime.value compareTo projectDateCreated.value) >= 0,
      activityData,
      s"Activity start time $startTime cannot be older than project creation date $projectDateCreated"
    )
  }
}

object ExecutionPlanner {

  private type ActivityData = (activities.StartTime, Person, CliVersion)

  def of(plan: Plan, activityTime: activities.StartTime, author: Person, project: RenkuProject): ExecutionPlanner =
    of(plan, activityTime, author, project.agent, project.topAncestorDateCreated)

  def of(plan:               Plan,
         activityTime:       activities.StartTime,
         author:             Person,
         cliVersion:         CliVersion,
         projectDateCreated: projects.DateCreated
  ): ExecutionPlanner =
    ExecutionPlanner(plan, (activityTime, author, cliVersion), List.empty, List.empty, List.empty, projectDateCreated)
}
