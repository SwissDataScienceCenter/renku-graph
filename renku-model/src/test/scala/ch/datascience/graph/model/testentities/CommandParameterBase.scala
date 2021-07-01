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
import ch.datascience.graph.model.RenkuBaseUrl
import CommandParameterBase.CommandInput.InputDefaultValue
import CommandParameterBase.CommandOutput.{FolderCreation, OutputDefaultValue}
import CommandParameterBase.CommandParameter.ParameterDefaultValue
import CommandParameterBase._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait CommandParameterBase {
  type DefaultValue

  val position:         Position
  val name:             Name
  val maybeDescription: Option[Description]
  val maybePrefix:      Option[Prefix]
  val defaultValue:     DefaultValue
  val runPlan:          RunPlan
}

object CommandParameterBase {

  final case class CommandParameter(position:         Position,
                                    name:             Name,
                                    maybeDescription: Option[Description],
                                    maybePrefix:      Option[Prefix],
                                    defaultValue:     ParameterDefaultValue,
                                    runPlan:          RunPlan
  ) extends CommandParameterBase {
    override type DefaultValue = ParameterDefaultValue
  }

  object CommandParameter {

    def from(value: ParameterDefaultValue): Position => RunPlan => CommandParameter =
      position =>
        runPlan =>
          CommandParameter(position,
                           Name(s"parameter_$position"),
                           maybeDescription = None,
                           maybePrefix = None,
                           defaultValue = value,
                           runPlan
          )

    final class ParameterDefaultValue private (val value: String) extends AnyVal with StringTinyType
    implicit object ParameterDefaultValue
        extends TinyTypeFactory[ParameterDefaultValue](new ParameterDefaultValue(_))
        with NonBlank

    implicit def commandParameterEncoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[CommandParameter] =
      JsonLDEncoder.instance {
        case parameter @ CommandParameter(position, name, maybeDescription, maybePrefix, defaultValue, _) =>
          JsonLD.entity(
            parameter.asEntityId,
            EntityTypes of (renku / "CommandParameter", renku / "CommandParameterBase"),
            schema / "name"         -> name.asJsonLD,
            schema / "description"  -> maybeDescription.asJsonLD,
            renku / "position"      -> position.asJsonLD,
            renku / "prefix"        -> maybePrefix.asJsonLD,
            schema / "defaultValue" -> defaultValue.asJsonLD,
            rdfs / "label"          -> s"""Command Parameter "$defaultValue"""".asJsonLD
          )
      }

    implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[CommandParameter] =
      EntityIdEncoder.instance(parameter =>
        parameter.runPlan.asEntityId.asUrlEntityId / "parameters" / parameter.position
      )
  }

  sealed trait CommandInputOrOutput extends CommandParameterBase

  sealed trait CommandInput extends CommandInputOrOutput {
    override type DefaultValue = InputDefaultValue
    val temporary:           Temporary
    val maybeEncodingFormat: Option[EncodingFormat]
  }

  object CommandInput {

    def fromLocation(defaultValue: Location): Position => RunPlan => CommandInput =
      from(InputDefaultValue(defaultValue))

    def streamedFromLocation(defaultValue: Location): Position => RunPlan => CommandInput =
      streamedFrom(InputDefaultValue(defaultValue))

    def from(defaultValue: InputDefaultValue): Position => RunPlan => LocationCommandInput =
      position =>
        runPlan =>
          LocationCommandInput(
            position,
            Name(s"input_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue,
            Temporary.nonTemporary,
            maybeEncodingFormat = None,
            runPlan
          )

    def streamedFrom(defaultValue: InputDefaultValue): Position => RunPlan => MappedCommandInput =
      position =>
        runPlan =>
          MappedCommandInput(
            position,
            Name(s"input_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue,
            Temporary.nonTemporary,
            maybeEncodingFormat = None,
            runPlan
          )

    final case class LocationCommandInput(position:            Position,
                                          name:                Name,
                                          maybeDescription:    Option[Description],
                                          maybePrefix:         Option[Prefix],
                                          defaultValue:        InputDefaultValue,
                                          temporary:           Temporary,
                                          maybeEncodingFormat: Option[EncodingFormat],
                                          runPlan:             RunPlan
    ) extends CommandInput

    final case class MappedCommandInput(position:            Position,
                                        name:                Name,
                                        maybeDescription:    Option[Description],
                                        maybePrefix:         Option[Prefix],
                                        defaultValue:        InputDefaultValue,
                                        temporary:           Temporary,
                                        maybeEncodingFormat: Option[EncodingFormat],
                                        runPlan:             RunPlan
    ) extends CommandInput {
      val mappedTo: IOStream.In = IOStream.StdIn
    }

    final case class InputDefaultValue(value: Location) extends TinyType { type V = Location }
    object InputDefaultValue {
      implicit val jsonLDEncoder: JsonLDEncoder[InputDefaultValue] =
        JsonLDEncoder[Location].contramap[InputDefaultValue](_.value)
    }

    implicit def commandInputEncoder[I <: CommandInput](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[I] =
      JsonLDEncoder.instance {
        case input @ LocationCommandInput(position,
                                          name,
                                          maybeDescription,
                                          maybePrefix,
                                          defaultValue,
                                          temporary,
                                          maybeEncodingFormat,
                                          _
            ) =>
          JsonLD.entity(
            input.asEntityId,
            EntityTypes of (renku / "CommandInput", renku / "CommandParameterBase"),
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "position"        -> position.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Input Template "$defaultValue"""".asJsonLD
          )
        case input @ MappedCommandInput(position,
                                        name,
                                        maybeDescription,
                                        maybePrefix,
                                        defaultValue,
                                        temporary,
                                        maybeEncodingFormat,
                                        _
            ) =>
          JsonLD.entity(
            input.asEntityId,
            EntityTypes of (renku / "CommandInput", renku / "CommandParameterBase"),
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "position"        -> position.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "mappedTo"        -> input.mappedTo.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Input Template "${input.defaultValue}"""".asJsonLD
          )
      }

    implicit def entityIdEncoder[I <: CommandInput](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[I] =
      EntityIdEncoder.instance(input => input.runPlan.asEntityId.asUrlEntityId / "inputs" / input.position)
  }

  sealed trait CommandOutput extends CommandInputOrOutput {
    override type DefaultValue = OutputDefaultValue
    val temporary:           Temporary
    val maybeEncodingFormat: Option[EncodingFormat]
    val folderCreation:      FolderCreation
  }

  object CommandOutput {

    def fromLocation(defaultValue: Location): Position => RunPlan => LocationCommandOutput =
      from(OutputDefaultValue(defaultValue))

    def from(defaultValue: OutputDefaultValue): Position => RunPlan => LocationCommandOutput =
      position =>
        runPlan =>
          LocationCommandOutput(
            position,
            Name(s"output_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue = defaultValue,
            FolderCreation.no,
            Temporary.nonTemporary,
            maybeEncodingFormat = None,
            runPlan
          )

    def streamedFromLocation(defaultValue: Location, stream: IOStream.Out): Position => RunPlan => MappedCommandOutput =
      streamedFrom(OutputDefaultValue(defaultValue), stream)

    def streamedFrom(defaultValue: OutputDefaultValue,
                     stream:       IOStream.Out
    ): Position => RunPlan => MappedCommandOutput =
      position =>
        runPlan =>
          MappedCommandOutput(
            position,
            Name(s"output_$position"),
            maybeDescription = None,
            maybePrefix = None,
            defaultValue = defaultValue,
            FolderCreation.no,
            Temporary.nonTemporary,
            maybeEncodingFormat = None,
            mappedTo = stream,
            runPlan
          )

    final case class LocationCommandOutput(position:            Position,
                                           name:                Name,
                                           maybeDescription:    Option[Description],
                                           maybePrefix:         Option[Prefix],
                                           defaultValue:        OutputDefaultValue,
                                           folderCreation:      FolderCreation,
                                           temporary:           Temporary,
                                           maybeEncodingFormat: Option[EncodingFormat],
                                           runPlan:             RunPlan
    ) extends CommandOutput

    final case class MappedCommandOutput(position:            Position,
                                         name:                Name,
                                         maybeDescription:    Option[Description],
                                         maybePrefix:         Option[Prefix],
                                         defaultValue:        OutputDefaultValue,
                                         folderCreation:      FolderCreation,
                                         temporary:           Temporary,
                                         maybeEncodingFormat: Option[EncodingFormat],
                                         mappedTo:            IOStream.Out,
                                         runPlan:             RunPlan
    ) extends CommandOutput

    final case class OutputDefaultValue(value: Location) extends TinyType { type V = Location }
    object OutputDefaultValue {
      implicit val jsonLDEncoder: JsonLDEncoder[OutputDefaultValue] = JsonLDEncoder[Location].contramap(_.value)
    }

    final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
    implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_)) {
      val no:  FolderCreation = FolderCreation(false)
      val yes: FolderCreation = FolderCreation(true)
    }

    implicit def commandOutputEncoder[O <: CommandOutput](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[O] =
      JsonLDEncoder.instance {
        case output @ LocationCommandOutput(position,
                                            name,
                                            maybeDescription,
                                            maybePrefix,
                                            defaultValue,
                                            folderCreation,
                                            temporary,
                                            maybeEncodingFormat,
                                            _
            ) =>
          JsonLD.entity(
            output.asEntityId,
            EntityTypes of (renku / "CommandOutput", renku / "CommandParameterBase"),
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "position"        -> position.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            renku / "createFolder"    -> folderCreation.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Output Template "$defaultValue"""".asJsonLD
          )
        case output @ MappedCommandOutput(position,
                                          name,
                                          maybeDescription,
                                          maybePrefix,
                                          defaultValue,
                                          folderCreation,
                                          temporary,
                                          maybeEncodingFormat,
                                          mappedTo,
                                          _
            ) =>
          JsonLD.entity(
            output.asEntityId,
            EntityTypes of (renku / "CommandOutput", renku / "CommandParameterBase"),
            schema / "name"           -> name.asJsonLD,
            schema / "description"    -> maybeDescription.asJsonLD,
            renku / "position"        -> position.asJsonLD,
            renku / "prefix"          -> maybePrefix.asJsonLD,
            schema / "defaultValue"   -> defaultValue.asJsonLD,
            renku / "mappedTo"        -> mappedTo.asJsonLD,
            renku / "isTemporary"     -> temporary.asJsonLD,
            renku / "createFolder"    -> folderCreation.asJsonLD,
            schema / "encodingFormat" -> maybeEncodingFormat.asJsonLD,
            rdfs / "label"            -> s"""Command Output Template "$defaultValue"""".asJsonLD
          )
      }

    implicit def entityIdEncoder[O <: CommandOutput](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[O] =
      EntityIdEncoder.instance(output => output.runPlan.asEntityId.asUrlEntityId / "outputs" / output.position)
  }

  final class Position private (val value: Int) extends AnyVal with IntTinyType
  implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt {
    val first:  Position = Position(1)
    val second: Position = Position(2)
    val third:  Position = Position(3)
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  final class EncodingFormat private (val value: String) extends AnyVal with StringTinyType
  implicit object EncodingFormat extends TinyTypeFactory[EncodingFormat](new EncodingFormat(_)) with NonBlank

  final class Temporary private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object Temporary extends TinyTypeFactory[Temporary](new Temporary(_)) {
    val temporary:    Temporary = Temporary(true)
    val nonTemporary: Temporary = Temporary(false)
  }

  sealed abstract class IOStream(val name: String Refined NonEmpty) {
    override lazy val toString: String = name.toString()
  }

  object IOStream {
    sealed trait In    extends IOStream
    sealed trait Out   extends IOStream
    case object StdIn  extends IOStream("stdin") with In
    case object StdOut extends IOStream("stdout") with Out
    case object StdErr extends IOStream("stderr") with Out

    implicit def encoder[IO <: IOStream](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[IO] =
      JsonLDEncoder.instance[IO] { stream =>
        JsonLD.entity(
          stream.asEntityId,
          EntityTypes of renku / "IOStream",
          renku / "streamType" -> stream.name.toString().asJsonLD
        )
      }

    implicit def entityIdEncoder[IO <: IOStream](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[IO] =
      EntityIdEncoder.instance(stream => EntityId.of(renkuBaseUrl / "iostreams" / stream.name.toString()))
  }

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank
}
