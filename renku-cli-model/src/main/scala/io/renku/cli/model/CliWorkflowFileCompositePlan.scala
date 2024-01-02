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

package io.renku.cli.model

import cats.data.NonEmptyList
import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.plans._
import io.renku.graph.model.{InvalidationTime, entityModel}
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliWorkflowFileCompositePlan(
    id:               ResourceId,
    name:             Name,
    description:      Option[Description],
    creators:         List[CliPerson],
    dateCreated:      DateCreated,
    dateModified:     DateModified,
    keywords:         List[Keyword],
    derivedFrom:      Option[DerivedFrom],
    invalidationTime: Option[InvalidationTime],
    plans:            NonEmptyList[CliWorkflowFileStepPlan],
    links:            List[CliParameterLink],
    mappings:         List[CliParameterMapping],
    path:             entityModel.Location.FileOrFolder
) extends CliModel {
  def asCliCompositePlan: CliCompositePlan =
    CliCompositePlan(
      id,
      name,
      description,
      creators,
      dateCreated,
      dateModified,
      keywords,
      derivedFrom,
      invalidationTime,
      plans.map(p => CliPlan(p.asCliStepPlan)),
      links,
      mappings
    )
}

object CliWorkflowFileCompositePlan {

  private val entityTypes: EntityTypes =
    EntityTypes.of(Renku.WorkflowFileCompositePlan, Renku.CompositePlan, Prov.Plan, Schema.Action, Schema.CreativeWork)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit def jsonLDDecoder: JsonLDEntityDecoder[CliWorkflowFileCompositePlan] =
    JsonLDDecoder.cacheableEntity(entityTypes, _.getEntityTypes.map(matchingEntityTypes)) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        name             <- cursor.downField(Schema.name).as[Name]
        description      <- cursor.downField(Schema.description).as[Option[Description]]
        creators         <- cursor.downField(Schema.creator).as[List[CliPerson]]
        dateCreated      <- cursor.downField(Schema.dateCreated).as[DateCreated]
        dateModified     <- cursor.downField(Schema.dateModified).as[DateModified]
        keywords         <- cursor.downField(Schema.keywords).as[List[Option[Keyword]]].map(_.flatten)
        derivedFrom      <- cursor.downField(Prov.wasDerivedFrom).as(JsonLDDecoder.decodeOption(DerivedFrom.ttDecoder))
        invalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
        links            <- cursor.downField(Renku.workflowLink).as[List[CliParameterLink]]
        mappings         <- cursor.downField(Renku.hasMappings).as[List[CliParameterMapping]]
        plans            <- cursor.downField(Renku.hasSubprocess).as[NonEmptyList[CliWorkflowFileStepPlan]]
        path             <- cursor.downField(Prov.atLocation).as[entityModel.Location.FileOrFolder]
      } yield CliWorkflowFileCompositePlan(
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
        mappings,
        path
      )
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliWorkflowFileCompositePlan] =
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
        Renku.hasSubprocess    -> plan.plans.asJsonLD,
        Prov.atLocation        -> plan.path.value.asJsonLD
      )
    }
}
