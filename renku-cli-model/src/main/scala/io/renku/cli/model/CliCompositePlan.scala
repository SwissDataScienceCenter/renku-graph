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
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.plans._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliCompositePlan(
    id:               ResourceId,
    name:             Name,
    description:      Option[Description],
    creators:         List[CliPerson],
    dateCreated:      DateCreated,
    dateModified:     Option[DateModified],
    keywords:         List[Keyword],
    derivedFrom:      Option[DerivedFrom],
    invalidationTime: Option[InvalidationTime],
    plans:            NonEmptyList[CliPlan],
    links:            List[CliParameterLink],
    mappings:         List[CliParameterMapping]
) extends CliModel

object CliCompositePlan {

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
        dateModified     <- cursor.downField(Schema.dateModified).as[Option[DateModified]]
        keywords         <- cursor.downField(Schema.keywords).as[List[Option[Keyword]]].map(_.flatten)
        derivedFrom      <- cursor.downField(Prov.wasDerivedFrom).as(JsonLDDecoder.decodeOption(DerivedFrom.ttDecoder))
        invalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
        links            <- cursor.downField(Renku.workflowLink).as[List[CliParameterLink]]
        mappings         <- cursor.downField(Renku.hasMappings).as[List[CliParameterMapping]]
        plans            <- cursor.downField(Renku.hasSubprocess).as[NonEmptyList[CliPlan]]
      } yield CliCompositePlan(
        resourceId,
        name,
        description,
        creators,
        dateCreated,
        dateModified,
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
        Schema.dateModified    -> plan.dateModified.asJsonLD,
        Schema.keywords        -> plan.keywords.asJsonLD,
        Prov.wasDerivedFrom    -> plan.derivedFrom.asJsonLD,
        Prov.invalidatedAtTime -> plan.invalidationTime.asJsonLD,
        Renku.workflowLink     -> plan.links.asJsonLD,
        Renku.hasMappings      -> plan.mappings.asJsonLD,
        Renku.hasSubprocess    -> plan.plans.asJsonLD
      )
    }
}
