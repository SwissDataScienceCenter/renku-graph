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

package io.renku.cli.model

import Ontologies.{Prov, Renku, Schema}
import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliCompositePlan.ChildPlan
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.plans._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliCompositePlan(
    id:               ResourceId,
    name:             Name,
    description:      Option[Description],
    creators:         List[CliPerson],
    dateCreated:      DateCreated,
    keywords:         List[Keyword],
    derivedFrom:      Option[DerivedFrom],
    invalidationTime: Option[InvalidationTime],
    plans:            NonEmptyList[ChildPlan],
    links:            List[CliParameterLink],
    mappings:         List[CliParameterMapping]
) extends CliModel

object CliCompositePlan {

  sealed trait ChildPlan {
    def fold[A](fa: CliPlan => A, fb: CliCompositePlan => A): A
  }

  object ChildPlan {
    final case class Step(value: CliPlan) extends ChildPlan {
      def fold[A](fa: CliPlan => A, fb: CliCompositePlan => A): A = fa(value)
    }
    final case class Composite(value: CliCompositePlan) extends ChildPlan {
      def fold[A](fa: CliPlan => A, fb: CliCompositePlan => A): A = fb(value)
    }

    def apply(value: CliPlan):          ChildPlan = Step(value)
    def apply(value: CliCompositePlan): ChildPlan = Composite(value)

    private val entityTypes: EntityTypes =
      EntityTypes.of(Prov.Plan, Schema.Action, Schema.CreativeWork)

    private def selectCandidates(ets: EntityTypes): Boolean =
      CliPlan.matchingEntityTypes(ets) || CliCompositePlan.matchingEntityTypes(ets)

    implicit def jsonLDDecoder: JsonLDDecoder[ChildPlan] = {
      val plan = CliPlan.jsonLDDecoder.emap(plan => ChildPlan(plan).asRight)
      val cp   = CliCompositePlan.jsonLDDecoder.emap(plan => ChildPlan(plan).asRight)

      JsonLDDecoder.cacheableEntity(entityTypes, _.getEntityTypes.map(selectCandidates)) { cursor =>
        val currentEntityTypes = cursor.getEntityTypes
        (currentEntityTypes.map(CliPlan.matchingEntityTypes),
         currentEntityTypes.map(CliCompositePlan.matchingEntityTypes)
        ).flatMapN {
          case (true, _) => plan(cursor)
          case (_, true) => cp(cursor)
          case _ =>
            DecodingFailure(
              s"Invalid entity for decoding child plans of a composite plan: $currentEntityTypes",
              Nil
            ).asLeft
        }
      }
    }

    implicit def jsonLDEncoder: JsonLDEncoder[ChildPlan] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes =
    EntityTypes.of(Renku.CompositePlan, Prov.Plan, Schema.Action, Schema.CreativeWork)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit def jsonLDDecoder: JsonLDDecoder[CliCompositePlan] =
    JsonLDDecoder.entity(entityTypes, _.getEntityTypes.map(matchingEntityTypes)) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        name             <- cursor.downField(Schema.name).as[Name]
        description      <- cursor.downField(Schema.description).as[Option[Description]]
        creators         <- cursor.downField(Schema.creator).as[List[CliPerson]]
        dateCreated      <- cursor.downField(Schema.dateCreated).as[DateCreated]
        keywords         <- cursor.downField(Schema.keywords).as[List[Option[Keyword]]].map(_.flatten)
        derivedFrom      <- cursor.downField(Prov.wasDerivedFrom).as(JsonLDDecoder.decodeOption(DerivedFrom.ttDecoder))
        invalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
        links            <- cursor.downField(Renku.workflowLink).as[List[CliParameterLink]]
        mappings         <- cursor.downField(Renku.hasMappings).as[List[CliParameterMapping]]
        plans            <- cursor.downField(Renku.hasSubprocess).as[NonEmptyList[ChildPlan]]
      } yield CliCompositePlan(
        resourceId,
        name,
        description,
        creators,
        dateCreated,
        keywords,
        derivedFrom,
        invalidationTime,
        plans,
        links,
        mappings
      )
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliCompositePlan] =
    JsonLDEncoder.instance { plan =>
      JsonLD.entity(
        plan.id.asEntityId,
        entityTypes,
        Schema.name            -> plan.name.asJsonLD,
        Schema.description     -> plan.description.asJsonLD,
        Schema.creator         -> plan.creators.asJsonLD,
        Schema.dateCreated     -> plan.dateCreated.asJsonLD,
        Schema.keywords        -> plan.keywords.asJsonLD,
        Prov.wasDerivedFrom    -> plan.derivedFrom.asJsonLD,
        Prov.invalidatedAtTime -> plan.invalidationTime.asJsonLD,
        Renku.workflowLink     -> plan.links.asJsonLD,
        Renku.hasMappings      -> plan.mappings.asJsonLD,
        Renku.hasSubprocess    -> plan.plans.asJsonLD
      )
    }
}
