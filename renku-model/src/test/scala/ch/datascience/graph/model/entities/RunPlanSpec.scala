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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.commandParameters.Position
import ch.datascience.graph.model.entities.Generators._
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities._
import ch.datascience.graph.model.{entities, runPlans}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RunPlanSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {

    "turn JsonLD RunPlan entity into the RunPlan object" in {
      forAll(runPlanObjects()) { runPlan =>
        runPlan.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.RunPlan]] shouldBe List(runPlan.to[entities.RunPlan]).asRight
      }
    }
  }

  private implicit lazy val parameterFactoryLists: Gen[List[Position => RunPlan => CommandParameterBase]] = for {
    parameters      <- commandParameterObjects.toGeneratorOfList()
    locationInputs  <- locationCommandInputObjects.toGeneratorOfList()
    mappedInputs    <- mappedCommandInputObjects.toGeneratorOfList()
    locationOutputs <- locationCommandOutputObjects.toGeneratorOfList()
    mappedOutputs   <- mappedCommandOutputObjects.toGeneratorOfList()
  } yield parameters ::: locationInputs ::: mappedInputs ::: locationOutputs ::: mappedOutputs

  private def runPlanObjects(
      projectGen: Gen[Project[ForksCount]] = projectEntities(visibilityAny)(anyForksCount)
  ): Gen[RunPlan] = for {
    name                     <- runPlanNames
    command                  <- runPlanCommands
    maybeDescription         <- runPlanDescriptions.toGeneratorOfOptions
    maybeProgrammingLanguage <- runPlanProgrammingLanguages.toGeneratorOfOptions
    keywords                 <- nonEmptyStrings().map(runPlans.Keyword).toGeneratorOfList()
    paramFactories           <- parameterFactoryLists
    successCodes             <- runPlanSuccessCodes.toGeneratorOfList()
    project                  <- projectGen
  } yield RunPlan(
    RunPlan.Id.generate,
    name,
    maybeDescription,
    command,
    maybeProgrammingLanguage,
    keywords,
    commandParameterFactories = paramFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    },
    successCodes,
    project
  )
}
