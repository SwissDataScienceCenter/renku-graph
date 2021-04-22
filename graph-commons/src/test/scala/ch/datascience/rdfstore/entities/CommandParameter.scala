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

package ch.datascience.rdfstore.entities

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory._
import ch.datascience.rdfstore.entities.CommandParameter.Mapping.IOStream.StdIn
import ch.datascience.rdfstore.entities.CommandParameter.Mapping._
import ch.datascience.rdfstore.entities.CommandParameter.Output.FolderCreation.no
import ch.datascience.rdfstore.entities.CommandParameter.Output.OutputFactory._
import ch.datascience.rdfstore.entities.CommandParameter.PositionInfo.Position
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

import scala.language.postfixOps

sealed abstract class CommandParameter(val runPlan: RunPlan, val maybePrefix: Option[Prefix])

object CommandParameter {

  sealed trait PositionInfo {
    self: CommandParameter =>
    val position: Position
  }

  object PositionInfo {

    final class Position private (val value: Int) extends AnyVal with IntTinyType
    implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt {
      val first:  Position = Position(1)
      val second: Position = Position(2)
      val third:  Position = Position(3)
    }

    private[entities] implicit val converter: PartialEntityConverter[PositionInfo] =
      new NoEntityIdPartialConverter[PositionInfo] {
        override def convert[T <: PositionInfo]: T => Either[Exception, PartialEntity] =
          entity =>
            PartialEntity(
              renku / "position" -> entity.position.asJsonLD
            ).asRight
      }
  }

  sealed trait Mapping {
    self: EntityCommandParameter =>
    type IO <: IOStream
    val mappedTo: IO
  }

  object Mapping {

    trait InputMapping extends Mapping {
      self: EntityCommandParameter =>
      type IO = IOStream.In
    }
    trait OutputMapping extends Mapping {
      self: EntityCommandParameter =>
      type IO = IOStream.Out
    }

    sealed abstract class IOStream(val name: String Refined NonEmpty) {
      override lazy val toString: String = name.toString()
    }

    object IOStream {
      trait In           extends IOStream
      trait Out          extends IOStream
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

    private[entities] implicit def converter[M <: Mapping](implicit
        renkuBaseUrl: RenkuBaseUrl
    ): PartialEntityConverter[M] = new NoEntityIdPartialConverter[M] {
      override def convert[T <: M]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            renku / "mappedTo" -> entity.mappedTo.asJsonLD
          ).asRight
    }
  }

  sealed abstract class EntityCommandParameter(override val runPlan:     RunPlan,
                                               override val maybePrefix: Option[Prefix],
                                               val entity:               Entity
  ) extends CommandParameter(runPlan, maybePrefix)

  final class Value private (val value: String) extends AnyVal with StringTinyType
  implicit object Value extends TinyTypeFactory[Value](new Value(_)) with NonBlank {
    def apply(location: Location): Value = Value(location.value)
  }

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank

  private[entities] implicit val converter: PartialEntityConverter[CommandParameter] =
    new NoEntityIdPartialConverter[CommandParameter] {
      override def convert[T <: CommandParameter]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of renku / "CommandParameter",
            renku / "prefix" -> entity.maybePrefix.asJsonLD
          ).asRight
    }

  final class Argument(override val position:    Position,
                       override val maybePrefix: Option[Prefix],
                       override val runPlan:     RunPlan
  ) extends CommandParameter(runPlan, maybePrefix)
      with PositionInfo {
    val value:                  Value  = Value("input_path")
    override lazy val toString: String = s"argument_$position"
  }

  object Argument {

    trait ArgumentFactory extends (Position => RunPlan => CommandParameter with Argument)

    def factory(maybePrefix: Option[Prefix] = None): ArgumentFactory =
      position => runPlan => new Argument(position, maybePrefix, runPlan)

    implicit def argumentEncoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[CommandParameter with Argument] =
      JsonLDEncoder.instance { argument =>
        JsonLD.entity(
          argument.asEntityId,
          EntityTypes of (renku / "CommandArgument", renku / "CommandParameter"),
          rdfs / "label"     -> s"""Command Argument "${argument.value}"""".asJsonLD,
          renku / "position" -> argument.position.asJsonLD,
          renku / "prefix"   -> argument.maybePrefix.asJsonLD,
          renku / "value"    -> argument.value.asJsonLD
        )
      }

    implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[CommandParameter with Argument] =
      EntityIdEncoder.instance(argument =>
        EntityId of renkuBaseUrl / "runs" / argument.runPlan.id / "arguments" / argument.position
      )
  }

  sealed trait Input {
    self: CommandParameter =>
    val role: Role
  }

  object Input {

    sealed trait InputFactory

    object InputFactory {
      trait PositionInputFactory extends InputFactory with (Position => RunPlan => EntityCommandParameter with Input)
      trait MappedInputFactory
          extends InputFactory
          with (Entity with RunPlan => EntityCommandParameter with Input with InputMapping)
      trait NoPositionInputFactory extends InputFactory with (RunPlan => EntityCommandParameter with Input)
    }

    def from(entity: Entity): PositionInputFactory = from(entity, maybePrefix = None)

    def from(entity: Entity, maybePrefix: Option[Prefix]): PositionInputFactory =
      inputPosition =>
        runPlan =>
          new EntityCommandParameter(runPlan, maybePrefix, entity) with Input with PositionInfo {
            override val position: Position = inputPosition
            override val role:     Role     = Role(s"inputs/$position")
          }

    def streamFrom(entity: Entity): MappedInputFactory =
      streamFrom(entity, maybePrefix = None)

    def streamFrom(entity: Entity, maybePrefix: Option[Prefix]): MappedInputFactory =
      runPlan =>
        new EntityCommandParameter(runPlan, maybePrefix, entity) with Input with InputMapping {
          override val mappedTo: IOStream.In = StdIn
          override val role:     Role        = Role(mappedTo.name.value)
        }

    def withoutPositionFrom(entity: Entity): NoPositionInputFactory =
      withoutPositionFrom(entity, maybePrefix = None)

    def withoutPositionFrom(entity: Entity, maybePrefix: Option[Prefix]): NoPositionInputFactory =
      runPlan =>
        new EntityCommandParameter(runPlan, maybePrefix, entity) with Input {
          override val role: Role = Role("input")
        }

    implicit def inputEncoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[CommandParameter with Input] =
      JsonLDEncoder.instance { input =>
        JsonLD.entity(
          input.asEntityId,
          EntityTypes of (renku / "CommandInputTemplate", renku / "CommandParameter"),
          rdfs / "label"     -> s"""Command Input Template "${input.value}"""".asJsonLD,
          renku / "position" -> input.position.asJsonLD,
          renku / "prefix"   -> input.maybePrefix.asJsonLD,
          renku / "consumes" -> input.entity.asJsonLD
        )
      }

    implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[CommandParameter with Input] =
      EntityIdEncoder.instance(input =>
        EntityId of renkuBaseUrl / "runs" / input.runPlan.id / "inputs" / input.position
      )

    implicit def inputEncoder(implicit
        renkuBaseUrl: RenkuBaseUrl,
        gitLabApiUrl: GitLabApiUrl
    ): JsonLDEncoder[CommandParameter with Input] =
      JsonLDEncoder.instance[CommandParameter with Input] {
        case entity: CommandParameter with Input with PositionInfo =>
          entity
            .asPartialJsonLD[CommandParameter]
            .combine(entity.asPartialJsonLD[PositionInfo])
            .combine(entity.asPartialJsonLD[CommandParameter with Input])
            .getOrFail
        case entity: CommandParameter with Input with InputMapping =>
          entity
            .asPartialJsonLD[CommandParameter]
            .combine(entity.asPartialJsonLD[InputMapping])
            .combine(entity.asPartialJsonLD[CommandParameter with Input])
            .getOrFail
        case entity: CommandParameter with Input =>
          entity
            .asPartialJsonLD[CommandParameter]
            .combine(entity.asPartialJsonLD[CommandParameter with Input])
            .getOrFail
        case entity => throw new NotImplementedError(s"Cannot serialize entity of type ${entity.getClass}")
      }
  }

  sealed trait Output {
    self: CommandParameter =>

    import CommandParameter.Output.FolderCreation

    val role:                 Role
    val outputFolderCreation: FolderCreation
  }

  object Output {

    sealed trait OutputFactory
    object OutputFactory {
      trait PositionOutputFactory
          extends OutputFactory
          with (Position => RunPlan => CommandParameter with Output with PositionInfo)
      trait MappedOutputFactory     extends OutputFactory with (RunPlan => CommandParameter with Output with OutputMapping)
      trait NoPositionOutputFactory extends OutputFactory with (RunPlan => CommandParameter with Output)
    }

    def factory(entityFactory: Activity => Entity): PositionOutputFactory =
      factory(entityFactory, maybePrefix = None, folderCreation = no)

    def factory(
        entityFactory:  Activity => Entity,
        maybePrefix:    Option[Prefix],
        folderCreation: FolderCreation
    ): PositionOutputFactory =
      activity =>
        outputPosition =>
          runPlan =>
            new EntityCommandParameter(runPlan, maybePrefix, entityFactory(activity)) with Output with PositionInfo {
              override val outputFolderCreation: FolderCreation = folderCreation
              override val position:             Position       = outputPosition
            }

    def streamFactory(
        entityFactory: Activity => Entity,
        to:            IOStream.Out
    ): MappedOutputFactory =
      streamFactory(entityFactory, maybePrefix = None, folderCreation = no, to)

    def streamFactory(
        entityFactory:  Activity => Entity,
        maybePrefix:    Option[Prefix],
        folderCreation: FolderCreation,
        to:             IOStream.Out
    ): MappedOutputFactory =
      activity =>
        runPlan =>
          new EntityCommandParameter(runPlan, maybePrefix, entityFactory(activity)) with Output with OutputMapping {
            override val outputFolderCreation: FolderCreation = folderCreation
            override val mappedTo:             IOStream.Out   = to
          }

    def withoutPositionFactory(entityFactory: Activity => Entity): NoPositionOutputFactory =
      withoutPositionFactory(entityFactory, maybePrefix = None, folderCreation = no)

    def withoutPositionFactory(
        entityFactory:  Activity => Entity,
        maybePrefix:    Option[Prefix],
        folderCreation: FolderCreation
    ): NoPositionOutputFactory =
      activity =>
        runPlan =>
          new EntityCommandParameter(maybePrefix, runPlan, entityFactory(activity)) with Output {
            override val outputFolderCreation: FolderCreation = folderCreation
          }

    implicit def outputEncoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[CommandParameter with Output] =
      JsonLDEncoder.instance { output =>
        JsonLD.entity(
          output.asEntityId,
          EntityTypes of (renku / "CommandOutputTemplate", renku / "CommandParameter"),
          rdfs / "label"         -> s"""Command Output Template "${output.value}"""".asJsonLD,
          renku / "createFolder" -> output.outputFolderCreation.asJsonLD,
          renku / "produces"     -> output.entity.asJsonLD,
          renku / "position"     -> output.position.asJsonLD,
          renku / "prefix"       -> output.maybePrefix.asJsonLD
        )
      }

    implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[CommandParameter with Output] =
      EntityIdEncoder.instance(output =>
        EntityId of renkuBaseUrl / "runs" / output.runPlan.id / "outputs" / output.position
      )

    implicit def outputEncoder(implicit
        renkuBaseUrl:  RenkuBaseUrl,
        gitLabApiUrl:  GitLabApiUrl,
        fusekiBaseUrl: FusekiBaseUrl
    ): JsonLDEncoder[CommandParameter with Output] =
      JsonLDEncoder.instance {
        case entity: CommandParameter with Output with PositionInfo =>
          entity
            .asPartialJsonLD[CommandParameter]
            .combine(entity.asPartialJsonLD[PositionInfo])
            .combine(entity.asPartialJsonLD[CommandParameter with Output])
            .getOrFail
        case entity: CommandParameter with Output with OutputMapping =>
          entity
            .asPartialJsonLD[CommandParameter]
            .combine(entity.asPartialJsonLD[OutputMapping])
            .combine(entity.asPartialJsonLD[CommandParameter with Output])
            .getOrFail
        case entity: CommandParameter with Output =>
          entity
            .asPartialJsonLD[CommandParameter]
            .combine(entity.asPartialJsonLD[CommandParameter with Output])
            .getOrFail
        case entity => throw new NotImplementedError(s"Cannot serialize entity of type ${entity.getClass}")
      }

    final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
    implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_)) {
      val no:  FolderCreation = FolderCreation(false)
      val yes: FolderCreation = FolderCreation(true)
    }
  }
}
