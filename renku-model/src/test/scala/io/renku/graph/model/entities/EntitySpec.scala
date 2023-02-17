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

import io.renku.cli.model.{CliActivity, CliEntity}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities.Entity.OutputEntity
import io.renku.graph.model.testentities.StepPlan.CommandParameters.CommandParameterFactory
import io.renku.graph.model.testentities._
import io.renku.graph.model.entities
import io.renku.graph.model.tools.AdditionalMatchers
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EntitySpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with DiffInstances
    with AdditionalMatchers {

  def activityGenerator(paramFactory: CommandParameterFactory): Gen[Activity] =
    projectCreatedDates().flatMap(date =>
      activityEntities(stepPlanEntities(planCommands, cliShapedPersons, paramFactory), cliShapedPersons)(date)
    )

  "fromCli" should {
    "turn CliInputEntity entity into the Entity object " in {
      forAll(inputEntities) { entity =>
        val cliEntity   = entity.to[CliEntity]
        val modelEntity = entity.to[entities.Entity.InputEntity]

        entities.Entity.fromCli(cliEntity) shouldMatchTo modelEntity
      }
    }

    "turn CliOutput entity into the Entity object " in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val activity    = activityGenerator(commandOutput).generateOne
        val cliEntities = activity.to[CliActivity].generations.map(_.entity)

        val result = cliEntities.map(entities.Entity.fromCli)

        result should contain allElementsOf activity.generations.map(
          _.entity.to[entities.Entity.OutputEntity]
        )
      }
    }

    "turn CliOutput entity with multiple Generations into the Entity object " in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val testActivity = activityGenerator(commandOutput).generateOne
        val updatedActivity = testActivity.copy(generationFactories =
          testActivity.generationFactories :+ (act =>
            Generation(Generation.Id.generate,
                       act,
                       gen => OutputEntity(entityLocations.generateOne, entityChecksums.generateOne, gen)
            )
          )
        )

        val result = updatedActivity.to[CliActivity].generations.map(_.entity).map(entities.Entity.fromCli)

        result should contain allElementsOf updatedActivity.generations.map(g => Entity.toEntity.apply(g.entity))
      }
    }
  }
}
