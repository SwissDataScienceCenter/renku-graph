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
import io.renku.generators.Generators.{nonEmptyStrings, timestampsNotInTheFuture}
import io.renku.graph.model.commandParameters.Position
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, plans}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PlanSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  (GraphClass.Default :: GraphClass.Persons :: Nil).foreach { implicit graphClass =>
    show"decode via graphClass=$graphClass" should {
      "turn JsonLD Plan entity into the Plan object" in {
        forAll(planObjects) { plan =>
          plan.asJsonLD.flatten
            .fold(throw _, identity)
            .cursor
            .as[List[entities.Plan]] shouldBe List(plan.to[entities.Plan]).asRight
        }
      }
    }
  }

  private implicit lazy val parameterFactoryLists: Gen[List[Position => Plan => CommandParameterBase]] = for {
    explicitParameters <- explicitCommandParameterObjects.toGeneratorOfList()
    locationInputs     <- locationCommandInputObjects.toGeneratorOfList()
    mappedInputs       <- mappedCommandInputObjects.toGeneratorOfList()
    locationOutputs    <- locationCommandOutputObjects.toGeneratorOfList()
    mappedOutputs      <- mappedCommandOutputObjects.toGeneratorOfList()
  } yield explicitParameters ::: locationInputs ::: mappedInputs ::: locationOutputs ::: mappedOutputs

  private lazy val planObjects: Gen[Plan] = for {
    name                     <- planNames
    maybeCommand             <- planCommands.toGeneratorOfOptions
    maybeDescription         <- planDescriptions.toGeneratorOfOptions
    dateCreated              <- timestampsNotInTheFuture.toGeneratorOf(plans.DateCreated)
    maybeProgrammingLanguage <- planProgrammingLanguages.toGeneratorOfOptions
    keywords                 <- nonEmptyStrings().map(plans.Keyword).toGeneratorOfList()
    paramFactories           <- parameterFactoryLists
    successCodes             <- planSuccessCodes.toGeneratorOfList()
    creators                 <- personEntities.toGeneratorOfList()
  } yield Plan(
    planIdentifiers.generateOne,
    name,
    maybeDescription,
    maybeCommand,
    creators.toSet,
    dateCreated,
    maybeProgrammingLanguage,
    keywords,
    commandParameterFactories = paramFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    },
    successCodes
  )
}
