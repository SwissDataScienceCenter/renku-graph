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

package io.renku.graph.model.entities

import io.renku.graph.model.Schemas._
import io.renku.graph.model.commandParameters._
import io.renku.jsonld.JsonLDDecoder

sealed trait StepPlanCommandParameter extends CommandParameterBase

sealed trait ExplicitParameter {
  self: StepPlanCommandParameter =>
  val position:         Position
  val maybeDescription: Option[Description]
}

object StepPlanCommandParameter {

  import io.renku.jsonld.ontology._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  sealed trait CommandParameter extends StepPlanCommandParameter {
    override type DefaultValue = ParameterDefaultValue
    val maybeDescription: Option[Description]
  }

  final case class ExplicitCommandParameter(resourceId:       ResourceId,
                                            position:         Position,
                                            name:             Name,
                                            maybeDescription: Option[Description],
                                            maybePrefix:      Option[Prefix],
                                            defaultValue:     ParameterDefaultValue
  ) extends CommandParameter
      with ExplicitParameter

  final case class ImplicitCommandParameter(resourceId:       ResourceId,
                                            name:             Name,
                                            maybeDescription: Option[Description],
                                            maybePrefix:      Option[Prefix],
                                            defaultValue:     ParameterDefaultValue
  ) extends CommandParameter

  object CommandParameter {

    private val entityTypes: EntityTypes = EntityTypes of (renku / "CommandParameter", renku / "CommandParameterBase")

    implicit def encoder[P <: CommandParameter]: JsonLDEncoder[P] = JsonLDEncoder.instance {
      case ExplicitCommandParameter(resourceId, position, name, maybeDescription, maybePrefix, defaultValue) =>
        JsonLD.entity(
          resourceId.asEntityId,
          entityTypes,
          renku / "position"      -> position.asJsonLD,
          schema / "name"         -> name.asJsonLD,
          schema / "description"  -> maybeDescription.asJsonLD,
          renku / "prefix"        -> maybePrefix.asJsonLD,
          schema / "defaultValue" -> defaultValue.asJsonLD
        )
      case ImplicitCommandParameter(resourceId, name, maybeDescription, maybePrefix, defaultValue) =>
        JsonLD.entity(
          resourceId.asEntityId,
          entityTypes,
          schema / "name"         -> name.asJsonLD,
          schema / "description"  -> maybeDescription.asJsonLD,
          renku / "prefix"        -> maybePrefix.asJsonLD,
          schema / "defaultValue" -> defaultValue.asJsonLD
        )
    }

    implicit lazy val decoder: JsonLDDecoder[CommandParameter] = JsonLDDecoder.entity(entityTypes) { cursor =>
      import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        maybePosition    <- cursor.downField(renku / "position").as[Option[Position]]
        name             <- cursor.downField(schema / "name").as[Name]
        maybeDescription <- cursor.downField(schema / "description").as[Option[Description]]
        maybePrefix      <- cursor.downField(renku / "prefix").as[Option[Prefix]]
        defaultValue     <- cursor.downField(schema / "defaultValue").as[ParameterDefaultValue]
      } yield maybePosition match {
        case Some(position) =>
          ExplicitCommandParameter(resourceId, position, name, maybeDescription, maybePrefix, defaultValue)
        case None =>
          ImplicitCommandParameter(resourceId, name, maybeDescription, maybePrefix, defaultValue)
      }
    }

    lazy val ontology: Type = Type.Def(
      Class(renku / "CommandParameter", ParentClass(renku / "CommandParameterBase")),
      DataProperty(renku / "position", xsd / "int"),
      DataProperty(schema / "name", xsd / "string"),
      DataProperty(schema / "description", xsd / "string"),
      DataProperty(renku / "prefix", xsd / "string"),
      DataProperty(schema / "defaultValue", xsd / "string")
    )
  }

  sealed trait CommandInputOrOutput extends StepPlanCommandParameter

  sealed trait CommandInput extends CommandInputOrOutput {
    override type DefaultValue = InputDefaultValue
    val maybeEncodingFormat: Option[EncodingFormat]
  }

  final case class LocationCommandInput(resourceId:          ResourceId,
                                        position:            Position,
                                        name:                Name,
                                        maybeDescription:    Option[Description],
                                        maybePrefix:         Option[Prefix],
                                        defaultValue:        InputDefaultValue,
                                        maybeEncodingFormat: Option[EncodingFormat]
  ) extends CommandInput
      with ExplicitParameter

  final case class MappedCommandInput(resourceId:          ResourceId,
                                      position:            Position,
                                      name:                Name,
                                      maybeDescription:    Option[Description],
                                      maybePrefix:         Option[Prefix],
                                      defaultValue:        InputDefaultValue,
                                      maybeEncodingFormat: Option[EncodingFormat],
                                      mappedTo:            IOStream.In
  ) extends CommandInput
      with ExplicitParameter

  final case class ImplicitCommandInput(resourceId:          ResourceId,
                                        name:                Name,
                                        maybePrefix:         Option[Prefix],
                                        defaultValue:        InputDefaultValue,
                                        maybeEncodingFormat: Option[EncodingFormat]
  ) extends CommandInput

  object CommandInput {

    val entityTypes: EntityTypes = EntityTypes of (renku / "CommandInput", renku / "CommandParameterBase")

    implicit def encoder[I <: CommandInput]: JsonLDEncoder[I] = JsonLDEncoder.instance {
      case LocationCommandInput(resourceId,
                                position,
                                name,
                                maybeDescription,
                                maybePrefix,
                                defaultValue,
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
          schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD
        )
      case MappedCommandInput(resourceId,
                              position,
                              name,
                              maybeDescription,
                              maybePrefix,
                              defaultValue,
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
          schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
          renku / "mappedTo"        -> mappedTo.asJsonLD
        )
      case ImplicitCommandInput(resourceId, name, maybePrefix, defaultValue, maybeEncodingFormat) =>
        JsonLD.entity(
          resourceId.asEntityId,
          entityTypes,
          schema / "name"           -> name.asJsonLD,
          renku / "prefix"          -> maybePrefix.asJsonLD,
          schema / "defaultValue"   -> defaultValue.asJsonLD,
          schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD
        )
    }

    implicit lazy val decoder: JsonLDDecoder[CommandInput] = JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId          <- cursor.downEntityId.as[ResourceId]
        maybePosition       <- cursor.downField(renku / "position").as[Option[Position]]
        name                <- cursor.downField(schema / "name").as[Name]
        maybeDescription    <- cursor.downField(schema / "description").as[Option[Description]]
        maybePrefix         <- cursor.downField(renku / "prefix").as[Option[Prefix]]
        defaultValue        <- cursor.downField(schema / "defaultValue").as[InputDefaultValue]
        maybeEncodingFormat <- cursor.downField(schema / "encodingFormat").as[Option[EncodingFormat]]
        maybeMappedTo       <- cursor.downField(renku / "mappedTo").as[Option[IOStream.In]]
      } yield createCommandInput(resourceId,
                                 maybePosition,
                                 name,
                                 maybeDescription,
                                 maybePrefix,
                                 defaultValue,
                                 maybeEncodingFormat,
                                 maybeMappedTo
      )
    }

    private def createCommandInput(resourceId:          ResourceId,
                                   maybePosition:       Option[Position],
                                   name:                Name,
                                   maybeDescription:    Option[Description],
                                   maybePrefix:         Option[Prefix],
                                   defaultValue:        InputDefaultValue,
                                   maybeEncodingFormat: Option[EncodingFormat],
                                   maybeMappedTo:       Option[IOStream.In]
    ) = (maybePosition, maybeMappedTo) match {
      case (Some(position), None) =>
        LocationCommandInput(resourceId,
                             position,
                             name,
                             maybeDescription,
                             maybePrefix,
                             defaultValue,
                             maybeEncodingFormat
        )
      case (maybePosition, Some(mappedTo)) =>
        MappedCommandInput(resourceId,
                           maybePosition.getOrElse(Position(1000)),
                           name,
                           maybeDescription,
                           maybePrefix,
                           defaultValue,
                           maybeEncodingFormat,
                           mappedTo
        )
      case _ => ImplicitCommandInput(resourceId, name, maybePrefix, defaultValue, maybeEncodingFormat)
    }

    lazy val ontology: Type = Type.Def(
      Class(renku / "CommandInput", ParentClass(renku / "CommandParameterBase")),
      ObjectProperties(ObjectProperty(renku / "mappedTo", IOStream.ontology)),
      DataProperties(
        DataProperty(renku / "position", xsd / "int"),
        DataProperty(schema / "name", xsd / "string"),
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(renku / "prefix", xsd / "string"),
        DataProperty(schema / "defaultValue", xsd / "string"),
        DataProperty(schema / "encodingFormat", xsd / "string")
      )
    )
  }

  sealed trait CommandOutput extends CommandInputOrOutput {
    override type DefaultValue = OutputDefaultValue
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
                                         maybeEncodingFormat: Option[EncodingFormat]
  ) extends CommandOutput
      with ExplicitParameter

  final case class MappedCommandOutput(resourceId:          ResourceId,
                                       position:            Position,
                                       name:                Name,
                                       maybeDescription:    Option[Description],
                                       maybePrefix:         Option[Prefix],
                                       defaultValue:        OutputDefaultValue,
                                       folderCreation:      FolderCreation,
                                       maybeEncodingFormat: Option[EncodingFormat],
                                       mappedTo:            IOStream.Out
  ) extends CommandOutput
      with ExplicitParameter

  case class ImplicitCommandOutput(resourceId:          ResourceId,
                                   name:                Name,
                                   maybePrefix:         Option[Prefix],
                                   defaultValue:        OutputDefaultValue,
                                   folderCreation:      FolderCreation,
                                   maybeEncodingFormat: Option[EncodingFormat]
  ) extends CommandOutput

  object CommandOutput {

    private val entityTypes = EntityTypes of (renku / "CommandOutput", renku / "CommandParameterBase")

    implicit def encoder[O <: CommandOutput]: JsonLDEncoder[O] = JsonLDEncoder.instance {
      case LocationCommandOutput(resourceId,
                                 position,
                                 name,
                                 maybeDescription,
                                 maybePrefix,
                                 defaultValue,
                                 folderCreation,
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
          renku / "createFolder"    -> folderCreation.asJsonLD,
          schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD
        )
      case MappedCommandOutput(resourceId,
                               position,
                               name,
                               maybeDescription,
                               maybePrefix,
                               defaultValue,
                               folderCreation,
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
          renku / "createFolder"    -> folderCreation.asJsonLD,
          schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD
        )

      case ImplicitCommandOutput(resourceId, name, maybePrefix, defaultValue, folderCreation, maybeEncodingFormat) =>
        JsonLD.entity(
          resourceId.asEntityId,
          entityTypes,
          schema / "name"           -> name.asJsonLD,
          renku / "prefix"          -> maybePrefix.asJsonLD,
          schema / "defaultValue"   -> defaultValue.asJsonLD,
          renku / "createFolder"    -> folderCreation.asJsonLD,
          schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD
        )
    }

    implicit lazy val decoder: JsonLDDecoder[CommandOutput] =
      JsonLDDecoder.entity(entityTypes) { cursor =>
        for {
          resourceId          <- cursor.downEntityId.as[ResourceId]
          maybePosition       <- cursor.downField(renku / "position").as[Option[Position]]
          name                <- cursor.downField(schema / "name").as[Name]
          maybeDescription    <- cursor.downField(schema / "description").as[Option[Description]]
          maybePrefix         <- cursor.downField(renku / "prefix").as[Option[Prefix]]
          defaultValue        <- cursor.downField(schema / "defaultValue").as[OutputDefaultValue]
          folderCreation      <- cursor.downField(renku / "createFolder").as[FolderCreation]
          maybeEncodingFormat <- cursor.downField(schema / "encodingFormat").as[Option[EncodingFormat]]
          maybeMappedTo       <- cursor.downField(renku / "mappedTo").as[Option[IOStream.Out]]
        } yield createCommandOutput(resourceId,
                                    maybePosition,
                                    name,
                                    maybeDescription,
                                    maybePrefix,
                                    defaultValue,
                                    folderCreation,
                                    maybeEncodingFormat,
                                    maybeMappedTo
        )
      }

    private def createCommandOutput(resourceId:          ResourceId,
                                    maybePosition:       Option[Position],
                                    name:                Name,
                                    maybeDescription:    Option[Description],
                                    maybePrefix:         Option[Prefix],
                                    defaultValue:        OutputDefaultValue,
                                    folderCreation:      FolderCreation,
                                    maybeEncodingFormat: Option[EncodingFormat],
                                    maybeMappedTo:       Option[IOStream.Out]
    ) = (maybePosition, maybeMappedTo) match {
      case (Some(position), None) =>
        LocationCommandOutput(resourceId,
                              position,
                              name,
                              maybeDescription,
                              maybePrefix,
                              defaultValue,
                              folderCreation,
                              maybeEncodingFormat
        )

      case (maybePosition, Some(mappedTo)) =>
        MappedCommandOutput(resourceId,
                            maybePosition.getOrElse(Position(1000)),
                            name,
                            maybeDescription,
                            maybePrefix,
                            defaultValue,
                            folderCreation,
                            maybeEncodingFormat,
                            mappedTo
        )
      case _ =>
        ImplicitCommandOutput(resourceId, name, maybePrefix, defaultValue, folderCreation, maybeEncodingFormat)
    }

    lazy val ontology: Type = Type.Def(
      Class(renku / "CommandOutput", ParentClass(renku / "CommandParameterBase")),
      ObjectProperties(ObjectProperty(renku / "mappedTo", IOStream.ontology)),
      DataProperties(
        DataProperty(renku / "position", xsd / "int"),
        DataProperty(schema / "name", xsd / "string"),
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(renku / "prefix", xsd / "string"),
        DataProperty(schema / "defaultValue", xsd / "string"),
        DataProperty(renku / "createFolder", xsd / "boolean"),
        DataProperty(schema / "encodingFormat", xsd / "string")
      )
    )
  }
}
