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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.entities
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CommandParameterBaseSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "CommandParameter.decode" should {

    "turn JsonLD of ExplicitCommandParameter entity into the ExplicitCommandParameter object" in {
      forAll(explicitCommandParameterObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.parameters.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandParameter]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandParameter]).asRight
      }
    }

    "turn JsonLD of ImplicitCommandParameter entity into the ExplicitCommandParameter object" in {
      forAll(implicitCommandParameterObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.parameters.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandParameter]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandParameter]).asRight
      }
    }
  }

  "CommandInput.decode" should {

    "turn JsonLD of LocationCommandInput entity into the LocationCommandInput object" in {
      forAll(locationCommandInputObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.inputs.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandInput]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandInput]).asRight
      }
    }

    "turn JsonLD of MappedCommandInput entity into the MappedCommandInput object" in {
      forAll(mappedCommandInputObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.inputs.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandInput]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandInput]).asRight
      }
    }

    "turn JsonLD of ImplicitCommandInput entity into the ImplicitCommandInput object" in {
      forAll(implicitCommandInputObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.inputs.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandInput]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandInput]).asRight
      }
    }
  }

  "CommandOutput.decode" should {

    "turn JsonLD of LocationCommandOutput entity into the LocationCommandOutput object" in {
      forAll(locationCommandOutputObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.outputs.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandOutput]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandOutput]).asRight
      }
    }

    "turn JsonLD of MappedCommandOutput entity into the MappedCommandOutput object" in {
      forAll(mappedCommandOutputObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.outputs.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandOutput]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandOutput]).asRight
      }
    }

    "turn JsonLD of ImplicitCommandOutput entity into the ImplicitCommandOutput object" in {
      forAll(implicitCommandOutputObjects) { parameterFactory =>
        val plan      = planEntities(parameterFactory)(planCommands)(projectCreatedDates().generateOne).generateOne
        val parameter = plan.outputs.head

        plan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.CommandParameterBase.CommandOutput]] shouldBe
          List(parameter.to[entities.CommandParameterBase.CommandOutput]).asRight
      }
    }
  }
}
