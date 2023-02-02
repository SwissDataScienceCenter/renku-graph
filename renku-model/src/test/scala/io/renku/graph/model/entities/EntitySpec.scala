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
import io.renku.cli.model.{CliActivity, CliDatasetFile, CliEntity}
import io.renku.cli.model.CliModel._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{datasetCreatedDates, projectCreatedDates}
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities.Entity.OutputEntity
import io.renku.graph.model.testentities.StepPlan.CommandParameters.CommandParameterFactory
import io.renku.graph.model.testentities._
import io.renku.graph.model.entities
import io.renku.jsonld.JsonLDDecoder._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import shapeless.HList

class EntitySpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  def activityGenerator(paramFactory: CommandParameterFactory): Gen[Activity] =
    projectCreatedDates().flatMap(date =>
      activityEntities(stepPlanEntities(planCommands, cliShapedPersons, paramFactory), cliShapedPersons)(date)
    )

  "decode" should {
    "turn JsonLD InputEntity entity into the Entity object " in {
      forAll(inputEntities) { entity =>
        entity
          .to[CliEntity]
          .asFlattenedJsonLD
          .cursor
          .as[List[entities.Entity]] shouldBe List(entity.to[entities.Entity.InputEntity]).asRight
      }
    }

    "turn JsonLD Output entity into the Entity object " in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val activity    = activityGenerator(commandOutput).generateOne
        val datasetPart = datasetPartEntities(datasetCreatedDates().generateOne.value).generateOne

        val Right(decodedEntities) =
          HList(activity.to[CliActivity], datasetPart.to[CliDatasetFile]).asFlattenedJsonLD.cursor
            .as[List[entities.Entity]]

        decodedEntities should contain allElementsOf activity.generations.map(
          _.entity.to[entities.Entity.OutputEntity]
        )
      }
    }

    "turn JsonLD Output entity into the OutputEntity object for the given generation Id " in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val activity     = activityGenerator(commandOutput).generateOne
        val prodActivity = activity.to[entities.Activity]
        val datasetPart  = datasetPartEntities(datasetCreatedDates().generateOne.value).generateOne

        val Right(decodedEntities) =
          HList(activity.to[CliActivity], datasetPart.to[CliDatasetFile]).asFlattenedJsonLD.cursor
            .as[List[entities.Entity.OutputEntity]](
              decodeList(entities.Entity.outputEntityDecoder(prodActivity.generations.head.resourceId))
            )

        decodedEntities should contain allElementsOf prodActivity.generations.map(_.entity)
      }
    }

    "turn JsonLD Output entity with multiple Generations into the Entity object " in {
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

        val cliActivity = updatedActivity.to[CliActivity]

        val Right(decodedEntities) = cliActivity.asFlattenedJsonLD.cursor
          .as[List[entities.Entity]]

        decodedEntities should contain allElementsOf updatedActivity.generations.map(g =>
          Entity.toEntity.apply(g.entity)
        )
      }
    }
  }
}
