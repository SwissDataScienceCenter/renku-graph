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

import Ontologies.{Renku, Schema}
import io.renku.graph.model.commandParameters._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliCommandInput(
    resourceId:     ResourceId,
    name:           Name,
    description:    Option[Description],
    prefix:         Option[Prefix],
    position:       Option[Position],
    defaultValue:   ParameterDefaultValue,
    mappedTo:       Option[CliMappedIOStream],
    encodingFormat: Option[EncodingFormat]
) extends CliCommandParameterBase
    with CliModel

object CliCommandInput {
  private val entityTypes: EntityTypes = EntityTypes of (Renku.CommandInput, Renku.CommandParameterBase)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit val jsonLDDecoder: JsonLDDecoder[CliCommandInput] =
    JsonLDDecoder.entity(entityTypes, _.getEntityTypes.map(matchingEntityTypes)) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        position         <- cursor.downField(Renku.position).as[Option[Position]]
        name             <- cursor.downField(Schema.name).as[Name]
        maybeDescription <- cursor.downField(Schema.description).as[Option[Description]]
        maybePrefix      <- cursor.downField(Renku.prefix).as[Option[Prefix]]
        defaultValue     <- cursor.downField(Schema.defaultValue).as[ParameterDefaultValue]
        encodingFormat   <- cursor.downField(Schema.encodingFormat).as[Option[EncodingFormat]]
        mappedTo         <- cursor.downField(Renku.mappedTo).as[Option[CliMappedIOStream]]
      } yield CliCommandInput(
        resourceId,
        name,
        maybeDescription,
        maybePrefix,
        position,
        defaultValue,
        mappedTo,
        encodingFormat
      )
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliCommandInput] =
    JsonLDEncoder.instance { param =>
      JsonLD.entity(
        param.resourceId.asEntityId,
        entityTypes,
        Renku.position        -> param.position.asJsonLD,
        Schema.name           -> param.name.asJsonLD,
        Schema.description    -> param.description.asJsonLD,
        Renku.prefix          -> param.prefix.asJsonLD,
        Schema.defaultValue   -> param.defaultValue.asJsonLD,
        Schema.encodingFormat -> param.encodingFormat.asJsonLD,
        Renku.mappedTo        -> param.mappedTo.asJsonLD
      )
    }
}
