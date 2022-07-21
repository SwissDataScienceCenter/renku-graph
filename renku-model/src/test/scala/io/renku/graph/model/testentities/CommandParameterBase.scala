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

package io.renku.graph.model.testentities

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.commandParameters.IOStream.{StdErr, StdIn, StdOut}
import io.renku.graph.model.commandParameters._
import io.renku.graph.model.entityModel.Location
import io.renku.graph.model.{RenkuUrl, commandParameters, entities}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait CommandParameterBase {
  type DefaultValue

  val name:         Name
  val maybePrefix:  Option[Prefix]
  val defaultValue: DefaultValue
  val plan:         Plan
}

sealed trait ExplicitParameter {
  self: CommandParameterBase =>
  val position:         Position
  val maybeDescription: Option[Description]
}

object CommandParameterBase {

  sealed trait CommandParameter extends CommandParameterBase {
    override type DefaultValue = ParameterDefaultValue
    val maybeDescription: Option[Description]
  }

  final case class ExplicitCommandParameter(position:         Position,
                                            name:             Name,
                                            maybeDescription: Option[Description],
                                            maybePrefix:      Option[Prefix],
                                            defaultValue:     ParameterDefaultValue,
                                            plan:             Plan
  ) extends CommandParameter
      with ExplicitParameter

  final case class ImplicitCommandParameter(name:             Name,
                                            maybeDescription: Option[Description],
                                            maybePrefix:      Option[Prefix],
                                            defaultValue:     ParameterDefaultValue,
                                            plan:             Plan
  ) extends CommandParameter

  object CommandParameter {

    def from(value: ParameterDefaultValue): Position => Plan => CommandParameter =
      position =>
        plan =>
          ExplicitCommandParameter(position,
                                   Name(s"parameter_$position"),
                                   maybeDescription = None,
                                   maybePrefix = None,
                                   defaultValue = value,
                                   plan
          )

    implicit def toEntitiesCommandParameter(implicit
        renkuUrl: RenkuUrl
    ): CommandParameter => entities.CommandParameterBase.CommandParameter = {
      case p @ ExplicitCommandParameter(position, name, maybeDesc, maybePrefix, defaultValue, _) =>
        entities.CommandParameterBase.ExplicitCommandParameter(
          commandParameters.ResourceId(p.asEntityId.show),
          position,
          name,
          maybeDesc,
          maybePrefix,
          defaultValue
        )
      case p @ ImplicitCommandParameter(name, maybeDesc, maybePrefix, defaultValue, _) =>
        entities.CommandParameterBase.ImplicitCommandParameter(
          commandParameters.ResourceId(p.asEntityId.show),
          name,
          maybeDesc,
          maybePrefix,
          defaultValue
        )
    }

    implicit def commandParameterEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[CommandParameter] =
      JsonLDEncoder.instance(_.to[entities.CommandParameterBase.CommandParameter].asJsonLD)

    implicit def entityIdEncoder[P <: CommandParameter](implicit renkuUrl: RenkuUrl): EntityIdEncoder[P] =
      EntityIdEncoder.instance {
        case p: ExplicitCommandParameter => p.plan.asEntityId.asUrlEntityId / "parameters" / p.position
        case p: ImplicitCommandParameter => p.plan.asEntityId.asUrlEntityId / "parameters" / p.name
      }
  }

  sealed trait CommandInputOrOutput extends CommandParameterBase

  sealed trait CommandInput extends CommandInputOrOutput {
    override type DefaultValue = InputDefaultValue
    val maybeEncodingFormat: Option[EncodingFormat]
  }

  object CommandInput {

    def fromLocation(defaultValue: Location): Position => Plan => CommandInput =
      from(InputDefaultValue(defaultValue))

    def streamedFromLocation(defaultValue: Location): Position => Plan => CommandInput =
      streamedFrom(InputDefaultValue(defaultValue))

    def from(defaultValue: InputDefaultValue): Position => Plan => LocationCommandInput =
      position =>
        plan =>
          LocationCommandInput(
            position,
            Name(s"input_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue,
            maybeEncodingFormat = None,
            plan
          )

    def streamedFrom(defaultValue: InputDefaultValue): Position => Plan => MappedCommandInput =
      position =>
        plan =>
          MappedCommandInput(
            position,
            Name(s"input_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue,
            maybeEncodingFormat = None,
            plan
          )

    final case class LocationCommandInput(position:            Position,
                                          name:                Name,
                                          maybeDescription:    Option[Description],
                                          maybePrefix:         Option[Prefix],
                                          defaultValue:        InputDefaultValue,
                                          maybeEncodingFormat: Option[EncodingFormat],
                                          plan:                Plan
    ) extends CommandInput
        with ExplicitParameter

    final case class MappedCommandInput(position:            Position,
                                        name:                Name,
                                        maybeDescription:    Option[Description],
                                        maybePrefix:         Option[Prefix],
                                        defaultValue:        InputDefaultValue,
                                        maybeEncodingFormat: Option[EncodingFormat],
                                        plan:                Plan
    )(implicit renkuUrl:                                     RenkuUrl)
        extends CommandInput
        with ExplicitParameter {
      val mappedTo: IOStream.In = IOStream.StdIn(IOStream.ResourceId((renkuUrl / "iostreams" / StdIn.name).value))
    }

    final case class ImplicitCommandInput(
        name:                Name,
        maybePrefix:         Option[Prefix],
        defaultValue:        InputDefaultValue,
        maybeEncodingFormat: Option[EncodingFormat],
        plan:                Plan
    ) extends CommandInput

    implicit def toEntitiesCommandInput(implicit
        renkuUrl: RenkuUrl
    ): CommandInput => entities.CommandParameterBase.CommandInput = {
      case parameter: LocationCommandInput =>
        entities.CommandParameterBase.LocationCommandInput(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.position,
          parameter.name,
          parameter.maybeDescription,
          parameter.maybePrefix,
          parameter.defaultValue,
          parameter.maybeEncodingFormat
        )
      case parameter: MappedCommandInput =>
        entities.CommandParameterBase.MappedCommandInput(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.position,
          parameter.name,
          parameter.maybeDescription,
          parameter.maybePrefix,
          parameter.defaultValue,
          parameter.maybeEncodingFormat,
          parameter.mappedTo
        )
      case parameter: ImplicitCommandInput =>
        entities.CommandParameterBase.ImplicitCommandInput(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.name,
          parameter.maybePrefix,
          parameter.defaultValue,
          parameter.maybeEncodingFormat
        )
    }

    implicit def commandInputEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[CommandInput] =
      JsonLDEncoder.instance(_.to[entities.CommandParameterBase.CommandInput].asJsonLD)

    implicit def entityIdEncoder[I <: CommandInput](implicit renkuUrl: RenkuUrl): EntityIdEncoder[I] =
      EntityIdEncoder.instance(input => input.plan.asEntityId.asUrlEntityId / "inputs" / input.name)
  }

  sealed trait CommandOutput extends CommandInputOrOutput {
    override type DefaultValue = OutputDefaultValue
    val maybeEncodingFormat: Option[EncodingFormat]
    val folderCreation:      FolderCreation
  }

  object CommandOutput {

    def fromLocation(defaultValue: Location): Position => Plan => LocationCommandOutput =
      from(OutputDefaultValue(defaultValue))

    def from(defaultValue: OutputDefaultValue): Position => Plan => LocationCommandOutput =
      position =>
        plan =>
          LocationCommandOutput(
            position,
            Name(s"output_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue = defaultValue,
            FolderCreation.no,
            maybeEncodingFormat = None,
            plan
          )

    def streamedFromLocation(defaultValue: Location, stream: IOStream.Out): Position => Plan => MappedCommandOutput =
      streamedFrom(OutputDefaultValue(defaultValue), stream)

    def stdOut(implicit renkuUrl: RenkuUrl): IOStream.StdOut =
      IOStream.StdOut(IOStream.ResourceId((renkuUrl / "iostreams" / StdOut.name).value))

    def stdErr(implicit renkuUrl: RenkuUrl): IOStream.StdErr =
      IOStream.StdErr(IOStream.ResourceId((renkuUrl / "iostreams" / StdErr.name).value))

    def streamedFrom(defaultValue: OutputDefaultValue, stream: IOStream.Out): Position => Plan => MappedCommandOutput =
      position =>
        plan =>
          MappedCommandOutput(
            position,
            Name(s"output_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue = defaultValue,
            FolderCreation.no,
            maybeEncodingFormat = None,
            mappedTo = stream,
            plan
          )

    final case class LocationCommandOutput(position:            Position,
                                           name:                Name,
                                           maybeDescription:    Option[Description],
                                           maybePrefix:         Option[Prefix],
                                           defaultValue:        OutputDefaultValue,
                                           folderCreation:      FolderCreation,
                                           maybeEncodingFormat: Option[EncodingFormat],
                                           plan:                Plan
    ) extends CommandOutput
        with ExplicitParameter

    final case class MappedCommandOutput(position:            Position,
                                         name:                Name,
                                         maybeDescription:    Option[Description],
                                         maybePrefix:         Option[Prefix],
                                         defaultValue:        OutputDefaultValue,
                                         folderCreation:      FolderCreation,
                                         maybeEncodingFormat: Option[EncodingFormat],
                                         mappedTo:            IOStream.Out,
                                         plan:                Plan
    ) extends CommandOutput
        with ExplicitParameter

    final case class ImplicitCommandOutput(
        name:                Name,
        maybePrefix:         Option[Prefix],
        defaultValue:        OutputDefaultValue,
        folderCreation:      FolderCreation,
        maybeEncodingFormat: Option[EncodingFormat],
        plan:                Plan
    ) extends CommandOutput

    implicit def toEntitiesCommandOutput(implicit
        renkuUrl: RenkuUrl
    ): CommandOutput => entities.CommandParameterBase.CommandOutput = {
      case parameter: LocationCommandOutput =>
        entities.CommandParameterBase.LocationCommandOutput(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.position,
          parameter.name,
          parameter.maybeDescription,
          parameter.maybePrefix,
          parameter.defaultValue,
          parameter.folderCreation,
          parameter.maybeEncodingFormat
        )
      case parameter: MappedCommandOutput =>
        entities.CommandParameterBase.MappedCommandOutput(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.position,
          parameter.name,
          parameter.maybeDescription,
          parameter.maybePrefix,
          parameter.defaultValue,
          parameter.folderCreation,
          parameter.maybeEncodingFormat,
          parameter.mappedTo
        )
      case parameter: ImplicitCommandOutput =>
        entities.CommandParameterBase.ImplicitCommandOutput(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.name,
          parameter.maybePrefix,
          parameter.defaultValue,
          parameter.folderCreation,
          parameter.maybeEncodingFormat
        )
    }

    implicit def commandOutputEncoder[O <: CommandOutput](implicit renkuUrl: RenkuUrl): JsonLDEncoder[O] =
      JsonLDEncoder.instance(_.to[entities.CommandParameterBase.CommandOutput].asJsonLD)

    implicit def entityIdEncoder[O <: CommandOutput](implicit renkuUrl: RenkuUrl): EntityIdEncoder[O] =
      EntityIdEncoder.instance(output => output.plan.asEntityId.asUrlEntityId / "outputs" / output.name)
  }
}
