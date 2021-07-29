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
import ch.datascience.graph.model.Schemas.{prov, renku, schema}
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.entities.Activity.entityTypes
import ch.datascience.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput}
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDEncoder, Reverse}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ActivitySpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "Activity.decode" should {

    "turn JsonLD Activity entity into the Activity object" in {
      forAll(executionPlanners(planEntities(), anyProjectEntities)) { executionPlanner =>
        val activity = executionPlanner.buildProvenanceUnsafe()
        activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Activity]] shouldBe List(activity.to[entities.Activity]).asRight
      }
    }

    "fail if there are Input Parameter Values for non-existing Usage Entities" in {
      val location = entityLocations.generateOne
      val activity =
        executionPlanners(planEntities(CommandInput.fromLocation(location)), anyProjectEntities).generateOne
          .planInputParameterValuesFromChecksum(location -> entityChecksums.generateOne)
          .buildProvenanceUnsafe()

      lazy val replaceEntityLocation: Vector[JsonLD] => JsonLD = { array =>
        JsonLD.arr(
          array.map { jsonLd =>
            jsonLd.cursor
              .as[entities.Entity]
              .map {
                case entity: entities.Entity.InputEntity =>
                  entity.copy(location = entityLocations.generateOne).asJsonLD
                case entity => entity.asJsonLD
              }
              .fold(_ => jsonLd, identity)
          }: _*
        )
      }

      val Left(error) = activity.asJsonLD.flatten
        .fold(throw _, _.asArray.fold(fail("JsonLD is not an array"))(replaceEntityLocation))
        .cursor
        .as[List[entities.Activity]]
      error              shouldBe a[DecodingFailure]
      error.getMessage() shouldBe s"No Usage found for InputParameterValue with $location"
    }

    "fail if there are Output Parameter Values for non-existing Generation Entities" in {
      val location = entityLocations.generateOne
      val activity = executionPlanners(
        planEntities(CommandOutput.fromLocation(location)),
        projectEntities(visibilityAny)(anyForksCount)
      ).generateOne
        .buildProvenanceUnsafe()

      lazy val replaceEntityLocation: Vector[JsonLD] => JsonLD = { array =>
        JsonLD.arr(
          array.map { jsonLd =>
            jsonLd.cursor
              .as[entities.Entity]
              .map {
                case entity: entities.Entity.OutputEntity =>
                  entity.copy(location = entityLocations.generateOne).asJsonLD
                case entity => entity.asJsonLD
              }
              .fold(_ => jsonLd, identity)
          }: _*
        )
      }

      val Left(error) = activity.asJsonLD.flatten
        .fold(throw _, _.asArray.fold(fail("JsonLD is not an array"))(replaceEntityLocation))
        .cursor
        .as[List[entities.Activity]]
      error              shouldBe a[DecodingFailure]
      error.getMessage() shouldBe s"No Generation found for OutputParameterValue with $location"
    }

    "fail if there is no Agent entity" in {
      val activity = executionPlanners(
        planEntities(),
        projectEntities(visibilityAny)(anyForksCount)
      ).generateOne
        .buildProvenanceUnsafe()
        .to[entities.Activity]

      val encoder = JsonLDEncoder.instance[entities.Activity] { entity =>
        JsonLD.entity(
          entity.resourceId.asEntityId,
          entityTypes,
          Reverse.ofJsonLDsUnsafe((prov / "activity") -> entity.generations.asJsonLD),
          prov / "startedAtTime"        -> entity.startTime.asJsonLD,
          prov / "endedAtTime"          -> entity.endTime.asJsonLD,
          prov / "wasAssociatedWith"    -> JsonLD.arr(entity.author.asJsonLD),
          prov / "qualifiedAssociation" -> entity.association.asJsonLD,
          prov / "qualifiedUsage"       -> entity.usages.asJsonLD,
          renku / "parameter"           -> entity.parameters.asJsonLD,
          schema / "isPartOf"           -> entity.projectResourceId.asEntityId.asJsonLD,
          renku / "order"               -> entity.order.asJsonLD
        )
      }

      val Left(error) = activity
        .asJsonLD(encoder)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Activity]]
      error         shouldBe a[DecodingFailure]
      error.message shouldBe s"Activity ${activity.resourceId} without or with multiple agents"
    }

    "fail if there is no Author entity" in {
      val activity = executionPlanners(
        planEntities(),
        projectEntities(visibilityAny)(anyForksCount)
      ).generateOne
        .buildProvenanceUnsafe()
        .to[entities.Activity]

      val encoder = JsonLDEncoder.instance[entities.Activity] { entity =>
        JsonLD.entity(
          entity.resourceId.asEntityId,
          entityTypes,
          Reverse.ofJsonLDsUnsafe((prov / "activity") -> entity.generations.asJsonLD),
          prov / "startedAtTime"        -> entity.startTime.asJsonLD,
          prov / "endedAtTime"          -> entity.endTime.asJsonLD,
          prov / "wasAssociatedWith"    -> JsonLD.arr(entity.agent.asJsonLD),
          prov / "qualifiedAssociation" -> entity.association.asJsonLD,
          prov / "qualifiedUsage"       -> entity.usages.asJsonLD,
          renku / "parameter"           -> entity.parameters.asJsonLD,
          schema / "isPartOf"           -> entity.projectResourceId.asEntityId.asJsonLD,
          renku / "order"               -> entity.order.asJsonLD
        )
      }

      val Left(error) = activity
        .asJsonLD(encoder)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Activity]]
      error         shouldBe a[DecodingFailure]
      error.message shouldBe s"Activity ${activity.resourceId} without or with multiple authors"
    }
  }
}
