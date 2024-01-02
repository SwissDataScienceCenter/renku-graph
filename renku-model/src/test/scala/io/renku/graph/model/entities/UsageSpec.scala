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
import io.renku.cli.model.CliActivity
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.testentities.StepPlanCommandParameter.CommandInput
import io.renku.graph.model.testentities._
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.{RenkuUrl, entities}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UsageSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with DiffInstances {

  "fromCli" should {
    implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne

    "turn CliUsage entity into the Usage object" in {
      forAll(entityLocations, entityChecksums) { (location, checksum) =>
        val activity = executionPlanners(
          stepPlanEntities(planCommands, cliShapedPersons, CommandInput.fromLocation(location)),
          projectCreatedDates().generateOne,
          cliShapedPersons
        ).generateOne
          .planInputParameterValuesFromChecksum(location -> checksum)
          .buildProvenanceUnsafe()

        val cliUsages = activity.to[CliActivity].usages
        val result    = cliUsages.traverse(entities.Usage.fromCli)

        result shouldMatchToValid activity.usages.map(_.to[entities.Usage])
      }
    }
  }
}
