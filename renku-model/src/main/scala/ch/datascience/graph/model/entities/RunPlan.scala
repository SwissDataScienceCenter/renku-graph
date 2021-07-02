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

import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import ch.datascience.graph.model.runPlans._
import ch.datascience.graph.model.{InvalidationTime, commandParameters, projects}
import io.renku.jsonld.JsonLDDecoder

final case class RunPlan(resourceId:               ResourceId,
                         name:                     Name,
                         maybeDescription:         Option[Description],
                         command:                  Command,
                         maybeProgrammingLanguage: Option[ProgrammingLanguage],
                         keywords:                 List[Keyword],
                         parameters:               List[CommandParameter],
                         inputs:                   List[CommandInput],
                         outputs:                  List[CommandOutput],
                         successCodes:             List[SuccessCode],
                         projectResourceId:        projects.ResourceId,
                         maybeInvalidationTime:    Option[InvalidationTime]
) extends RunPlanOps

sealed trait RunPlanOps {
  self: RunPlan =>

  def findParameter(parameterId: commandParameters.ResourceId): Option[CommandParameter] =
    parameters.find(_.resourceId == parameterId)

  def findInput(parameterId: commandParameters.ResourceId): Option[CommandInput] =
    inputs.find(_.resourceId == parameterId)

  def findOutput(parameterId: commandParameters.ResourceId): Option[CommandOutput] =
    outputs.find(_.resourceId == parameterId)
}

object RunPlan {
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  private val entityTypes = EntityTypes.of(prov / "Plan", renku / "Run")

  implicit lazy val encoder: JsonLDEncoder[RunPlan] = JsonLDEncoder.instance { plan =>
    JsonLD.entity(
      plan.resourceId.asEntityId,
      entityTypes,
      schema / "name"                -> plan.name.asJsonLD,
      schema / "description"         -> plan.maybeDescription.asJsonLD,
      renku / "command"              -> plan.command.asJsonLD,
      schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
      schema / "keywords"            -> plan.keywords.asJsonLD,
      renku / "hasArguments"         -> plan.parameters.asJsonLD,
      renku / "hasInputs"            -> plan.inputs.asJsonLD,
      renku / "hasOutputs"           -> plan.outputs.asJsonLD,
      renku / "successCodes"         -> plan.successCodes.asJsonLD,
      schema / "isPartOf"            -> plan.projectResourceId.asEntityId.asJsonLD,
      prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
    )
  }

  implicit lazy val decoder: JsonLDDecoder[RunPlan] = JsonLDDecoder.entity(entityTypes) { cursor =>
    import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._
    for {
      resourceId            <- cursor.downEntityId.as[ResourceId]
      name                  <- cursor.downField(schema / "name").as[Name]
      maybeDescription      <- cursor.downField(schema / "description").as[Option[Description]]
      command               <- cursor.downField(renku / "command").as[Command]
      maybeProgrammingLang  <- cursor.downField(schema / "programmingLanguage").as[Option[ProgrammingLanguage]]
      keywords              <- cursor.downField(schema / "keywords").as[List[Keyword]]
      parameters            <- cursor.downField(renku / "hasArguments").as[List[CommandParameter]]
      inputs                <- cursor.downField(renku / "hasInputs").as[List[CommandInput]]
      outputs               <- cursor.downField(renku / "hasOutputs").as[List[CommandOutput]]
      successCodes          <- cursor.downField(renku / "successCodes").as[List[SuccessCode]]
      projectId             <- cursor.downField(schema / "isPartOf").downEntityId.as[projects.ResourceId]
      maybeInvalidationTime <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
    } yield RunPlan(resourceId,
                    name,
                    maybeDescription,
                    command,
                    maybeProgrammingLang,
                    keywords,
                    parameters,
                    inputs,
                    outputs,
                    successCodes,
                    projectId,
                    maybeInvalidationTime
    )
  }
}
