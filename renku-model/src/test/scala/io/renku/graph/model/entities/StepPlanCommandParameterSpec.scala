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
import io.renku.cli.model
import io.renku.cli.model.CliStepPlan
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities.StepPlan.CommandParameters.CommandParameterFactory
import io.renku.graph.model.testentities._
import io.renku.graph.model.entities
import io.renku.graph.model.tools.AdditionalMatchers
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StepPlanCommandParameterSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with DiffInstances {

  def planGenerator(parameterFactory: CommandParameterFactory) =
    projectCreatedDates().flatMap { date =>
      stepPlanEntities(planCommands, cliShapedPersons, parameterFactory)(date)
    }

  "StepPlanCommandParameter.fromCli" should {

    "turn cli data of ExplicitCommandParameter entity into the ExplicitCommandParameter object" in {
      forAll(explicitCommandParameterObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliParams = plan.to[CliStepPlan].parameters
        val result    = cliParams.traverse(entities.StepPlanCommandParameter.CommandParameter.fromCli)
        result shouldMatchToValid plan.parameters.map(_.to[entities.StepPlanCommandParameter.CommandParameter])
      }
    }

    "turn cli data of ImplicitCommandParameter entity into the ExplicitCommandParameter object" in {
      forAll(implicitCommandParameterObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliParams = plan.to[CliStepPlan].parameters
        val result    = cliParams.traverse(entities.StepPlanCommandParameter.CommandParameter.fromCli)
        result shouldMatchToValid plan.parameters.map(_.to[entities.StepPlanCommandParameter.CommandParameter])
      }
    }
  }

  "CommandInput.fromCli" should {

    "turn cli data of LocationCommandInput entity into the LocationCommandInput object" in {
      forAll(locationCommandInputObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliInputs = plan.to[model.CliStepPlan].inputs
        val result    = cliInputs.traverse(entities.StepPlanCommandParameter.CommandInput.fromCli)
        result shouldMatchToValid plan.inputs.map(_.to[entities.StepPlanCommandParameter.CommandInput])
      }
    }

    "turn cli data of MappedCommandInput entity into the MappedCommandInput object" in {
      forAll(mappedCommandInputObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliInputs = plan.to[model.CliStepPlan].inputs
        val result    = cliInputs.traverse(entities.StepPlanCommandParameter.CommandInput.fromCli)
        result shouldMatchToValid plan.inputs.map(_.to[entities.StepPlanCommandParameter.CommandInput])
      }
    }

    "turn cli data of ImplicitCommandInput entity into the ImplicitCommandInput object" in {
      forAll(implicitCommandInputObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliInputs = plan.to[model.CliStepPlan].inputs
        val result    = cliInputs.traverse(entities.StepPlanCommandParameter.CommandInput.fromCli)
        result shouldMatchToValid plan.inputs.map(_.to[entities.StepPlanCommandParameter.CommandInput])
      }
    }
  }

  show"CommandOutput.fromCli" should {

    "turn cli data of LocationCommandOutput entity into the LocationCommandOutput object" in {
      forAll(locationCommandOutputObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliInputs = plan.to[model.CliStepPlan].outputs
        val result    = cliInputs.traverse(entities.StepPlanCommandParameter.CommandOutput.fromCli)
        result shouldMatchToValid plan.outputs.map(_.to[entities.StepPlanCommandParameter.CommandOutput])
      }
    }

    "turn cli data of MappedCommandOutput entity into the MappedCommandOutput object" in {
      forAll(mappedCommandOutputObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliInputs = plan.to[model.CliStepPlan].outputs
        val result    = cliInputs.traverse(entities.StepPlanCommandParameter.CommandOutput.fromCli)
        result shouldMatchToValid plan.outputs.map(_.to[entities.StepPlanCommandParameter.CommandOutput])
      }
    }

    "turn cli data of ImplicitCommandOutput entity into the ImplicitCommandOutput object" in {
      forAll(implicitCommandOutputObjects) { parameterFactory =>
        val plan      = planGenerator(parameterFactory).generateOne
        val cliInputs = plan.to[model.CliStepPlan].outputs
        val result    = cliInputs.traverse(entities.StepPlanCommandParameter.CommandOutput.fromCli)
        result shouldMatchToValid plan.outputs.map(_.to[entities.StepPlanCommandParameter.CommandOutput])
      }
    }
  }
}
