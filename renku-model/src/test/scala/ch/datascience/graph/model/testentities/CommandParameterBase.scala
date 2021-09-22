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

package ch.datascience.graph.model.testentities

import cats.syntax.all._
import ch.datascience.graph.model.commandParameters.IOStream.{StdErr, StdIn, StdOut}
import ch.datascience.graph.model.commandParameters._
import ch.datascience.graph.model.entityModel.Location
import ch.datascience.graph.model.{RenkuBaseUrl, commandParameters, entities}
import eu.timepit.refined.auto._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait CommandParameterBase {
  type DefaultValue

  val position:         Position
  val name:             Name
  val maybeDescription: Option[Description]
  val maybePrefix:      Option[Prefix]
  val defaultValue:     DefaultValue
  val plan:             Plan
}

object CommandParameterBase {

  final case class CommandParameter(position:         Position,
                                    name:             Name,
                                    maybeDescription: Option[Description],
                                    maybePrefix:      Option[Prefix],
                                    defaultValue:     ParameterDefaultValue,
                                    plan:             Plan
  ) extends CommandParameterBase {
    override type DefaultValue = ParameterDefaultValue
  }

  object CommandParameter {

    def from(value: ParameterDefaultValue): Position => Plan => CommandParameter =
      position =>
        plan =>
          CommandParameter(position,
                           Name(s"parameter_$position"),
                           maybeDescription = None,
                           maybePrefix = None,
                           defaultValue = value,
                           plan
          )

    implicit def toEntitiesCommandParameter(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): CommandParameter => entities.CommandParameterBase.CommandParameter =
      parameter =>
        entities.CommandParameterBase.CommandParameter(
          commandParameters.ResourceId(parameter.asEntityId.show),
          parameter.position,
          parameter.name,
          parameter.maybeDescription,
          parameter.maybePrefix,
          parameter.defaultValue
        )

    implicit def commandParameterEncoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[CommandParameter] =
      JsonLDEncoder.instance(_.to[entities.CommandParameterBase.CommandParameter].asJsonLD)

    implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[CommandParameter] =
      EntityIdEncoder.instance(parameter => parameter.plan.asEntityId.asUrlEntityId / "parameters" / parameter.position)
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

    final case class MappedCommandInput(position:            Position,
                                        name:                Name,
                                        maybeDescription:    Option[Description],
                                        maybePrefix:         Option[Prefix],
                                        defaultValue:        InputDefaultValue,
                                        maybeEncodingFormat: Option[EncodingFormat],
                                        plan:                Plan
    )(implicit renkuBaseUrl:                                 RenkuBaseUrl)
        extends CommandInput {
      val mappedTo: IOStream.In = IOStream.StdIn(IOStream.ResourceId((renkuBaseUrl / "iostreams" / StdIn.name).value))
    }

    implicit def toEntitiesCommandInput(implicit
        renkuBaseUrl: RenkuBaseUrl
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
    }

    implicit def commandInputEncoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[CommandInput] =
      JsonLDEncoder.instance(_.to[entities.CommandParameterBase.CommandInput].asJsonLD)

    implicit def entityIdEncoder[I <: CommandInput](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[I] =
      EntityIdEncoder.instance(input => input.plan.asEntityId.asUrlEntityId / "inputs" / input.position)
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

    def stdOut(implicit renkuBaseUrl: RenkuBaseUrl): IOStream.StdOut =
      IOStream.StdOut(IOStream.ResourceId((renkuBaseUrl / "iostreams" / StdOut.name).value))

    def stdErr(implicit renkuBaseUrl: RenkuBaseUrl): IOStream.StdErr =
      IOStream.StdErr(IOStream.ResourceId((renkuBaseUrl / "iostreams" / StdErr.name).value))

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

    implicit def toEntitiesCommandOutput(implicit
        renkuBaseUrl: RenkuBaseUrl
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
    }

    implicit def commandOutputEncoder[O <: CommandOutput](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[O] =
      JsonLDEncoder.instance(_.to[entities.CommandParameterBase.CommandOutput].asJsonLD)

    implicit def entityIdEncoder[O <: CommandOutput](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[O] =
      EntityIdEncoder.instance(output => output.plan.asEntityId.asUrlEntityId / "outputs" / output.position)
  }
}
