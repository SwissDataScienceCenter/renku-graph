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
import ch.datascience.graph.model.commandParameters._
import io.renku.jsonld.JsonLDDecoder

sealed trait CommandParameterBase {
  type DefaultValue

  val resourceId:       ResourceId
  val position:         Position
  val name:             Name
  val maybeDescription: Option[Description]
  val maybePrefix:      Option[Prefix]
  val defaultValue:     DefaultValue
}

object CommandParameterBase {

  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  final case class CommandParameter(resourceId:       ResourceId,
                                    position:         Position,
                                    name:             Name,
                                    maybeDescription: Option[Description],
                                    maybePrefix:      Option[Prefix],
                                    defaultValue:     ParameterDefaultValue
  ) extends CommandParameterBase {
    override type DefaultValue = ParameterDefaultValue
  }

  object CommandParameter {

    private val entityTypes: EntityTypes = EntityTypes of (renku / "CommandParameter", renku / "CommandParameterBase")

    implicit lazy val commandParameterEncoder: JsonLDEncoder[CommandParameter] =
      JsonLDEncoder.instance {
        case CommandParameter(resourceId, position, name, maybeDescription, maybePrefix, defaultValue) =>
          JsonLD.entity(
            resourceId.asEntityId,
            entityTypes,
            renku / "position"      -> position.asJsonLD,
            schema / "name"         -> name.asJsonLD,
            schema / "description"  -> maybeDescription.asJsonLD,
            renku / "prefix"        -> maybePrefix.asJsonLD,
            schema / "defaultValue" -> defaultValue.asJsonLD,
            rdfs / "label"          -> s"""Command Parameter "$defaultValue"""".asJsonLD
          )
      }

    implicit lazy val decoder: JsonLDDecoder[CommandParameter] = JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        position         <- cursor.downField(renku / "position").as[Position]
        name             <- cursor.downField(schema / "name").as[Name]
        maybeDescription <- cursor.downField(schema / "description").as[Option[Description]]
        maybePrefix      <- cursor.downField(renku / "prefix").as[Option[Prefix]]
        defaultValue     <- cursor.downField(schema / "defaultValue").as[ParameterDefaultValue]
      } yield CommandParameter(resourceId, position, name, maybeDescription, maybePrefix, defaultValue)
    }
  }

  sealed trait CommandInputOrOutput extends CommandParameterBase

  sealed trait CommandInput extends CommandInputOrOutput {
    override type DefaultValue = InputDefaultValue
    val temporary:           Temporary
    val maybeEncodingFormat: Option[EncodingFormat]
  }

  final case class LocationCommandInput(resourceId:          ResourceId,
                                        position:            Position,
                                        name:                Name,
                                        maybeDescription:    Option[Description],
                                        maybePrefix:         Option[Prefix],
                                        defaultValue:        InputDefaultValue,
                                        temporary:           Temporary,
                                        maybeEncodingFormat: Option[EncodingFormat]
  ) extends CommandInput

  final case class MappedCommandInput(resourceId:          ResourceId,
                                      position:            Position,
                                      name:                Name,
                                      maybeDescription:    Option[Description],
                                      maybePrefix:         Option[Prefix],
                                      defaultValue:        InputDefaultValue,
                                      temporary:           Temporary,
                                      maybeEncodingFormat: Option[EncodingFormat],
                                      mappedTo:            IOStream.In
  ) extends CommandInput

  object CommandInput {

    val entityTypes: EntityTypes = EntityTypes of (renku / "CommandInput", renku / "CommandParameterBase")

    implicit def commandInputEncoder[I <: CommandInput]: JsonLDEncoder[I] =
      JsonLDEncoder.instance {
        case LocationCommandInput(resourceId,
                                  position,
                                  name,
                                  maybeDescription,
                                  maybePrefix,
                                  defaultValue,
                                  temporary,
                                  maybeEncodingFormat
            ) =>
          JsonLD.entity(
            resourceId.asEntityId,
            entityTypes,
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "position"        -> position.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Input Template "$defaultValue"""".asJsonLD
          )
        case MappedCommandInput(resourceId,
                                position,
                                name,
                                maybeDescription,
                                maybePrefix,
                                defaultValue,
                                temporary,
                                maybeEncodingFormat,
                                mappedTo
            ) =>
          JsonLD.entity(
            resourceId.asEntityId,
            entityTypes,
            renku / "position"        -> position.asJsonLD,
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            renku / "mappedTo"        -> mappedTo.asJsonLD,
            rdfs / "label"            -> s"""Command Input Template "$defaultValue"""".asJsonLD
          )
      }

    implicit lazy val decoder: JsonLDDecoder[CommandInput] = JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId          <- cursor.downEntityId.as[ResourceId]
        position            <- cursor.downField(renku / "position").as[Position]
        name                <- cursor.downField(schema / "name").as[Name]
        maybeDescription    <- cursor.downField(schema / "description").as[Option[Description]]
        maybePrefix         <- cursor.downField(renku / "prefix").as[Option[Prefix]]
        defaultValue        <- cursor.downField(schema / "defaultValue").as[InputDefaultValue]
        temporary           <- cursor.downField(renku / "isTemporary").as[Temporary]
        maybeEncodingFormat <- cursor.downField(schema / "encodingFormat").as[Option[EncodingFormat]]
        maybeMappedTo       <- cursor.downField(renku / "mappedTo").as[Option[IOStream.In]]
      } yield maybeMappedTo match {
        case None =>
          LocationCommandInput(resourceId,
                               position,
                               name,
                               maybeDescription,
                               maybePrefix,
                               defaultValue,
                               temporary,
                               maybeEncodingFormat
          )
        case Some(mappedTo) =>
          MappedCommandInput(resourceId,
                             position,
                             name,
                             maybeDescription,
                             maybePrefix,
                             defaultValue,
                             temporary,
                             maybeEncodingFormat,
                             mappedTo
          )
      }
    }
  }

  sealed trait CommandOutput extends CommandInputOrOutput {
    override type DefaultValue = OutputDefaultValue
    val temporary:           Temporary
    val maybeEncodingFormat: Option[EncodingFormat]
    val folderCreation:      FolderCreation
  }

  final case class LocationCommandOutput(resourceId:          ResourceId,
                                         position:            Position,
                                         name:                Name,
                                         maybeDescription:    Option[Description],
                                         maybePrefix:         Option[Prefix],
                                         defaultValue:        OutputDefaultValue,
                                         folderCreation:      FolderCreation,
                                         temporary:           Temporary,
                                         maybeEncodingFormat: Option[EncodingFormat]
  ) extends CommandOutput

  final case class MappedCommandOutput(resourceId:          ResourceId,
                                       position:            Position,
                                       name:                Name,
                                       maybeDescription:    Option[Description],
                                       maybePrefix:         Option[Prefix],
                                       defaultValue:        OutputDefaultValue,
                                       folderCreation:      FolderCreation,
                                       temporary:           Temporary,
                                       maybeEncodingFormat: Option[EncodingFormat],
                                       mappedTo:            IOStream.Out
  ) extends CommandOutput

  object CommandOutput {

    private val entityTypes = EntityTypes of (renku / "CommandOutput", renku / "CommandParameterBase")

    implicit def commandOutputEncoder[O <: CommandOutput]: JsonLDEncoder[O] =
      JsonLDEncoder.instance {
        case LocationCommandOutput(resourceId,
                                   position,
                                   name,
                                   maybeDescription,
                                   maybePrefix,
                                   defaultValue,
                                   folderCreation,
                                   temporary,
                                   maybeEncodingFormat
            ) =>
          JsonLD.entity(
            resourceId.asEntityId,
            entityTypes,
            renku / "position"        -> position.asJsonLD,
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            renku / "createFolder"    -> folderCreation.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Output Template "$defaultValue"""".asJsonLD
          )
        case MappedCommandOutput(resourceId,
                                 position,
                                 name,
                                 maybeDescription,
                                 maybePrefix,
                                 defaultValue,
                                 folderCreation,
                                 temporary,
                                 maybeEncodingFormat,
                                 mappedTo
            ) =>
          JsonLD.entity(
            resourceId.asEntityId,
            entityTypes,
            renku / "position"        -> position.asJsonLD,
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "mappedTo"        -> mappedTo.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            renku / "createFolder"    -> folderCreation.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Output Template "$defaultValue"""".asJsonLD
          )
      }

    implicit lazy val decoder: JsonLDDecoder[CommandOutput] = JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId          <- cursor.downEntityId.as[ResourceId]
        position            <- cursor.downField(renku / "position").as[Position]
        name                <- cursor.downField(schema / "name").as[Name]
        maybeDescription    <- cursor.downField(schema / "description").as[Option[Description]]
        maybePrefix         <- cursor.downField(renku / "prefix").as[Option[Prefix]]
        defaultValue        <- cursor.downField(schema / "defaultValue").as[OutputDefaultValue]
        folderCreation      <- cursor.downField(renku / "createFolder").as[FolderCreation]
        temporary           <- cursor.downField(renku / "isTemporary").as[Temporary]
        maybeEncodingFormat <- cursor.downField(schema / "encodingFormat").as[Option[EncodingFormat]]
        maybeMappedTo       <- cursor.downField(renku / "mappedTo").as[Option[IOStream.Out]]
      } yield maybeMappedTo match {
        case None =>
          LocationCommandOutput(resourceId,
                                position,
                                name,
                                maybeDescription,
                                maybePrefix,
                                defaultValue,
                                folderCreation,
                                temporary,
                                maybeEncodingFormat
          )
        case Some(mappedTo) =>
          MappedCommandOutput(resourceId,
                              position,
                              name,
                              maybeDescription,
                              maybePrefix,
                              defaultValue,
                              folderCreation,
                              temporary,
                              maybeEncodingFormat,
                              mappedTo
          )
      }
    }
  }
}
