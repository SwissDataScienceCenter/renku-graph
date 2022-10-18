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
import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptyStrings, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.Position
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.jsonld.parser._
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PlanSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  (GraphClass.Default :: GraphClass.Persons :: Nil).foreach { implicit graphClass =>
    show"decode via graphClass=$graphClass" should {
      "turn JsonLD Plan entity into the Plan object" in {
        forAll(stepPlanObjects) { plan =>
          plan.asJsonLD.flatten
            .fold(throw _, identity)
            .cursor
            .as[List[entities.Plan]] shouldBe List(plan.to[entities.Plan]).asRight
        }
      }

      "decode if invalidation after the creation date" in {

        val plan = stepPlanObjects
          .map(p =>
            p.invalidate(
              timestampsNotInTheFuture(butYoungerThan = p.dateCreated.value).generateAs(InvalidationTime)
            ).fold(err => fail(err.intercalate("; ")), identity)
          )
          .generateOne
          .to[entities.Plan]

        plan.asJsonLD.flatten.fold(throw _, identity).cursor.as[List[entities.Plan]] shouldBe List(plan).asRight
      }

      "fail if invalidation done before the creation date" in {

        val plan = stepPlanObjects.generateOne.to[entities.Plan]

        val invalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)
        val jsonLD = parse {
          plan.asJsonLD.toJson.deepMerge(
            Json.obj(
              (prov / "invalidatedAtTime").show -> json"""{"@value": ${invalidationTime.show}}"""
            )
          )
        }.flatMap(_.flatten).fold(throw _, identity)

        val Left(message) = jsonLD.cursor.as[List[entities.Plan]].leftMap(_.message)
        message should include {
          show"Invalidation time $invalidationTime on StepPlan with id: ${plan.resourceId} is older than dateCreated ${plan.dateCreated}"
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

  private lazy val stepPlanObjects: Gen[StepPlan] = for {
    name                     <- planNames
    maybeCommand             <- planCommands.toGeneratorOfOptions
    maybeDescription         <- planDescriptions.toGeneratorOfOptions
    dateCreated              <- timestampsNotInTheFuture.toGeneratorOf(plans.DateCreated)
    maybeProgrammingLanguage <- planProgrammingLanguages.toGeneratorOfOptions
    keywords                 <- nonEmptyStrings().map(plans.Keyword).toGeneratorOfList()
    paramFactories           <- parameterFactoryLists
    successCodes             <- planSuccessCodes.toGeneratorOfList()
    creators                 <- personEntities.toGeneratorOfList()
  } yield StepPlan(
    planIdentifiers.generateOne,
    name,
    maybeDescription,
    creators.toSet,
    dateCreated,
    keywords,
    maybeCommand,
    maybeProgrammingLanguage,
    successCodes,
    commandParameterFactories = paramFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    }
  )
}
