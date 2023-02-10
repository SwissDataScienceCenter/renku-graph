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

import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.plans._
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliStepPlan(
    id:               ResourceId,
    name:             Name,
    description:      Option[Description],
    creators:         List[CliPerson],
    dateCreated:      DateCreated,
    dateModified:     DateModified,
    keywords:         List[Keyword],
    command:          Option[Command],
    parameters:       List[CliCommandParameter],
    inputs:           List[CliCommandInput],
    outputs:          List[CliCommandOutput],
    successCodes:     List[SuccessCode],
    derivedFrom:      Option[DerivedFrom],
    invalidationTime: Option[InvalidationTime]
) extends CliModel

object CliStepPlan {

  private val entityTypes: EntityTypes =
    EntityTypes.of(Renku.Plan, Prov.Plan, Schema.Action, Schema.CreativeWork)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit val jsonLDDecoder: JsonLDEntityDecoder[CliStepPlan] =
    jsonLDDecoderWith(_.getEntityTypes.map(matchingEntityTypes))

  /** Decodes also "subtypes" of step plans, i.e. the WorkflowFilePlan, viewing them as a step plan. */
  val jsonLDDecoderLenientTyped: JsonLDEntityDecoder[CliStepPlan] =
    jsonLDDecoderWith(_.getEntityTypes.map(_ contains entityTypes))

  private def jsonLDDecoderWith(filter: Cursor => Result[Boolean]): JsonLDEntityDecoder[CliStepPlan] =
    JsonLDDecoder.cacheableEntity(entityTypes, filter) { cursor =>
      for {
        resourceId   <- cursor.downEntityId.as[ResourceId]
        name         <- cursor.downField(Schema.name).as[Name]
        description  <- cursor.downField(Schema.description).as[Option[Description]]
        command      <- cursor.downField(Renku.command).as[Option[Command]]
        creators     <- cursor.downField(Schema.creator).as[List[CliPerson]]
        dateCreated  <- cursor.downField(Schema.dateCreated).as[DateCreated]
        dateModified <- cursor.downField(Schema.dateModified).as[DateModified]
        keywords     <- cursor.downField(Schema.keywords).as[List[Option[Keyword]]].map(_.flatten)
        parameters   <- cursor.downField(Renku.hasArguments).as[List[CliCommandParameter]]
        inputs       <- cursor.downField(Renku.hasInputs).as[List[CliCommandInput]]
        outputs      <- cursor.downField(Renku.hasOutputs).as[List[CliCommandOutput]]
        successCodes <- cursor.downField(Renku.successCodes).as[List[SuccessCode]]
        derivedFrom <-
          cursor.downField(Prov.wasDerivedFrom).as(JsonLDDecoder.decodeOption(DerivedFrom.ttDecoder))
        invalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
      } yield CliStepPlan(
        resourceId,
        name,
        description,
        creators,
        dateCreated,
        dateModified,
        keywords,
        command,
        parameters,
        inputs,
        outputs,
        successCodes,
        derivedFrom,
        invalidationTime
      )
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliStepPlan] =
    JsonLDEncoder.instance { plan =>
      JsonLD.entity(
        plan.id.asEntityId,
        entityTypes,
        Schema.name            -> plan.name.asJsonLD,
        Schema.description     -> plan.description.asJsonLD,
        Renku.command          -> plan.command.asJsonLD,
        Schema.creator         -> plan.creators.asJsonLD,
        Schema.dateCreated     -> plan.dateCreated.asJsonLD,
        Schema.dateModified    -> plan.dateModified.asJsonLD,
        Schema.keywords        -> plan.keywords.asJsonLD,
        Renku.hasArguments     -> plan.parameters.asJsonLD,
        Renku.hasInputs        -> plan.inputs.asJsonLD,
        Renku.hasOutputs       -> plan.outputs.asJsonLD,
        Renku.successCodes     -> plan.successCodes.asJsonLD,
        Prov.wasDerivedFrom    -> plan.derivedFrom.asJsonLD,
        Prov.invalidatedAtTime -> plan.invalidationTime.asJsonLD
      )
    }
}
