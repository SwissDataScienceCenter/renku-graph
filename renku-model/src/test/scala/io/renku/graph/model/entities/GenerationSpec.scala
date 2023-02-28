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

import io.renku.cli.model.CliGeneration
import io.renku.cli.model.generators.GenerationGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.entities
import io.renku.graph.model.tools.AdditionalMatchers
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GenerationSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with DiffInstances
    with AdditionalMatchers {

  "fromCli" should {
    "fail on input entities" in {
      val cliGeneration = GenerationGenerators.generationGen(activityResourceIdGen).generateOne
      val invalidGen    = CliGeneration.Lenses.entityGenerationIds.set(Nil)(cliGeneration)
      val result        = entities.Generation.fromCli(invalidGen)
      result should beInvalidWithMessageIncluding(
        "Expected output entity for a Generation, but got: InputEntity"
      )
    }

    "turn valid CliGeneration into Generation object" in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val testActivity =
          activityEntities(stepPlanEntities(planCommands, cliShapedPersons, commandOutput), cliShapedPersons)(
            projectCreatedDates().generateOne
          ).generateOne

        val generation    = testActivity.generations.head
        val cliGeneration = generation.to[CliGeneration]
        entities.Generation.fromCli(cliGeneration) shouldMatchToValid generation.to[entities.Generation]
      }
    }
  }
}
