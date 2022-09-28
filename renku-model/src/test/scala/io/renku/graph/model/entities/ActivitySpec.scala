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
import io.circe.DecodingFailure
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.Schemas.{prov, renku}
import io.renku.graph.model.entities.Activity.entityTypes
import io.renku.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDEncoder, Reverse}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ActivitySpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD Activity entity into the Activity object" in {
      forAll(activityEntities(planEntities())(projectCreatedDates().generateOne)) { activity =>
        JsonLD
          .arr(activity.asJsonLD, activity.association.plan.asJsonLD)
          .flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Activity]] shouldBe List(activity.to[entities.Activity]).asRight
      }
    }

    "fail if there are Input Parameter Values for non-existing Usage Entities" in {
      val location = entityLocations.generateOne
      val activity =
        executionPlanners(planEntities(CommandInput.fromLocation(location)),
                          anyRenkuProjectEntities.generateOne
        ).generateOne
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

      val Left(error) = JsonLD
        .arr(activity.asJsonLD, activity.association.plan.asJsonLD)
        .flatten
        .fold(throw _, _.asArray.fold(fail("JsonLD is not an array"))(replaceEntityLocation))
        .cursor
        .as[List[entities.Activity]]

      error            shouldBe a[DecodingFailure]
      error.getMessage() should endWith(s"No Usage found for CommandInputValue with $location")
    }

    "fail if there are Output Parameter Values for non-existing Generation Entities" in {
      val location = entityLocations.generateOne
      val activity = executionPlanners(
        planEntities(CommandOutput.fromLocation(location)),
        anyRenkuProjectEntities.generateOne
      ).generateOne.buildProvenanceUnsafe()

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

      val Left(error) = JsonLD
        .arr(activity.asJsonLD, activity.association.plan.asJsonLD)
        .flatten
        .fold(throw _, _.asArray.fold(fail("JsonLD is not an array"))(replaceEntityLocation))
        .cursor
        .as[List[entities.Activity]]

      error            shouldBe a[DecodingFailure]
      error.getMessage() should endWith(s"No Generation found for CommandOutputValue with $location")
    }

    "fail if there is no Agent entity" in {
      val activity = executionPlanners(planEntities(), anyRenkuProjectEntities.generateOne).generateOne
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
          renku / "parameter"           -> entity.parameters.asJsonLD
        )
      }

      val Left(error) = JsonLD
        .arr(activity.asJsonLD(encoder), activity.association.plan.asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Activity]]

      error       shouldBe a[DecodingFailure]
      error.message should endWith(s"Activity ${activity.resourceId} without or with multiple agents")
    }

    "fail if there is no Author entity" in {
      val activity = executionPlanners(planEntities(), anyRenkuProjectEntities.generateOne).generateOne
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
          renku / "parameter"           -> entity.parameters.asJsonLD
        )
      }

      val Left(error) = JsonLD
        .arr(activity.asJsonLD(encoder), activity.association.plan.asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Activity]]

      error       shouldBe a[DecodingFailure]
      error.message should endWith(s"Activity ${activity.resourceId} without or with multiple authors")
    }
  }

  "encode for the Default Graph" should {
    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD with all the relevant properties" in {
      val activity = executionPlanners(planEntities(), anyRenkuProjectEntities.generateOne).generateOne
        .buildProvenanceUnsafe()
        .to[entities.Activity]

      activity.asJsonLD shouldBe JsonLD.entity(
        activity.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe((prov / "activity") -> activity.generations.asJsonLD),
        prov / "startedAtTime"        -> activity.startTime.asJsonLD,
        prov / "endedAtTime"          -> activity.endTime.asJsonLD,
        prov / "wasAssociatedWith"    -> JsonLD.arr(activity.agent.asJsonLD, activity.author.asJsonLD),
        prov / "qualifiedAssociation" -> activity.association.asJsonLD,
        prov / "qualifiedUsage"       -> activity.usages.asJsonLD,
        renku / "parameter"           -> activity.parameters.asJsonLD
      )
    }
  }

  "encode for the Project Graph" should {
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD with all the relevant properties and only links to Person entities" in {
      val activity = executionPlanners(planEntities(), anyRenkuProjectEntities.generateOne).generateOne
        .buildProvenanceUnsafe()
        .to[entities.Activity]

      activity.asJsonLD shouldBe JsonLD.entity(
        activity.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe((prov / "activity") -> activity.generations.asJsonLD),
        prov / "startedAtTime" -> activity.startTime.asJsonLD,
        prov / "endedAtTime"   -> activity.endTime.asJsonLD,
        prov / "wasAssociatedWith" -> JsonLD.arr(activity.agent.asJsonLD,
                                                 activity.author.resourceId.asEntityId.asJsonLD
        ),
        prov / "qualifiedAssociation" -> activity.association.asJsonLD,
        prov / "qualifiedUsage"       -> activity.usages.asJsonLD,
        renku / "parameter"           -> activity.parameters.asJsonLD
      )
    }
  }

  "entityFunctions.findAllPersons" should {

    "return Activity's author and Association's agent if exists" in {
      val activity = executionPlanners(planEntities(), anyRenkuProjectEntities.generateOne).generateOne
        .buildProvenanceUnsafe()
        .to[entities.Activity]

      val maybeAssociationPersons: Set[entities.Person] = activity.association.agent match {
        case p: entities.Person => Set(p)
        case _ => Set.empty[entities.Person]
      }

      EntityFunctions[entities.Activity].findAllPersons(activity) shouldBe
        Set(activity.author) ++ maybeAssociationPersons
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {
      val activity = executionPlanners(planEntities(), anyRenkuProjectEntities.generateOne).generateOne
        .buildProvenanceUnsafe()
        .to[entities.Activity]

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Activity].encoder(graph)

      activity.asJsonLD(functionsEncoder) shouldBe activity.asJsonLD
    }
  }
}
