/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.util.UUID.randomUUID

import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
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
import io.renku.jsonld.{EntityId, EntityTypes, JsonLDEncoder}

import scala.language.postfixOps

sealed abstract class CommandParameter(val maybePrefix: Option[Prefix], val runPlan: Entity with RunPlan) {
  val value: Value
}

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

      private[entities] implicit def converter[IO <: IOStream](implicit
          renkuBaseUrl: RenkuBaseUrl
      ): PartialEntityConverter[IO] =
        new PartialEntityConverter[IO] {
          override def convert[T <: IO]: T => Either[Exception, PartialEntity] =
            stream =>
              PartialEntity(
                EntityTypes of renku / "IOStream",
                renku / "streamType" -> stream.name.toString().asJsonLD
              ).asRight

          override def toEntityId: IO => Option[EntityId] =
            e => EntityId.of(renkuBaseUrl / "iostreams" / e.name.toString()).some
        }

      implicit def encoder[IO <: IOStream](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[IO] =
        JsonLDEncoder.instance[IO](_.asPartialJsonLD[IO].getOrFail)
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

  sealed abstract class EntityCommandParameter(override val maybePrefix: Option[Prefix],
                                               override val runPlan:     Entity with RunPlan,
                                               val entity:               Entity with Artifact
  ) extends CommandParameter(maybePrefix, runPlan) {
    override lazy val value: Value = Value(entity.location)
  }

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
                       override val runPlan:     Entity with RunPlan
  ) extends CommandParameter(maybePrefix, runPlan)
      with PositionInfo {
    override val value:         Value  = Value("input_path")
    override lazy val toString: String = s"argument_$position"
  }

  object Argument {

    trait ArgumentFactory extends (Position => Entity with RunPlan => CommandParameter with Argument)

    def factory(maybePrefix: Option[Prefix] = None): ArgumentFactory =
      position => runPlan => new Argument(position, maybePrefix, runPlan)

    private implicit def converter(implicit
        renkuBaseUrl:  RenkuBaseUrl,
        fusekiBaseUrl: FusekiBaseUrl
    ): PartialEntityConverter[CommandParameter with Argument] =
      new PartialEntityConverter[CommandParameter with Argument] {

        override def convert[T <: CommandParameter with Argument]: T => Either[Exception, PartialEntity] =
          argument =>
            PartialEntity(
              EntityTypes of renku / "CommandArgument",
              rdfs / "label" -> s"""Command Argument "${argument.value}"""".asJsonLD
            ).asRight

        override lazy val toEntityId: CommandParameter with Argument => Option[EntityId] = argument =>
          argument.runPlan.getEntityId map (_ / "arguments" / argument)
      }

    implicit def argumentEncoder(implicit
        renkuBaseUrl:  RenkuBaseUrl,
        fusekiBaseUrl: FusekiBaseUrl
    ): JsonLDEncoder[CommandParameter with Argument] =
      JsonLDEncoder.instance[CommandParameter with Argument] { entity =>
        entity
          .asPartialJsonLD[CommandParameter]
          .combine(entity.asPartialJsonLD[PositionInfo])
          .combine(entity.asPartialJsonLD[CommandParameter with Argument])
          .getOrFail
      }
  }

  sealed trait Input {
    self: CommandParameter =>

    val maybeStep:            Option[Step]
    protected val identifier: String
    override lazy val toString: String = s"input_$identifier"
  }

  object Input {

    sealed trait InputFactory

    object InputFactory {
      trait ActivityPositionInputFactory
          extends InputFactory
          with (Activity => Position => Entity with RunPlan => EntityCommandParameter with Input with PositionInfo)
      trait PositionInputFactory
          extends InputFactory
          with (Position => Entity with RunPlan => EntityCommandParameter with Input)
      trait MappedInputFactory
          extends InputFactory
          with (Entity with RunPlan => EntityCommandParameter with Input with InputMapping)
      trait NoPositionInputFactory extends InputFactory with (Entity with RunPlan => EntityCommandParameter with Input)
    }

    def from(entity: Entity with Artifact): PositionInputFactory =
      from(entity, maybePrefix = None, maybeUsedIn = None)

    def from(entity: Entity with Artifact, usedIn: Step): PositionInputFactory =
      from(entity, maybePrefix = None, maybeUsedIn = usedIn.some)

    def from(entity:      Entity with Artifact,
             maybePrefix: Option[Prefix],
             maybeUsedIn: Option[Step]
    ): PositionInputFactory =
      positionArg =>
        runPlan =>
          new EntityCommandParameter(maybePrefix, runPlan, entity) with Input with PositionInfo {
            protected override val identifier: String       = positionArg.toString
            override val maybeStep:            Option[Step] = maybeUsedIn
            override val position:             Position     = positionArg
          }

    def streamFrom(entity: Entity with Artifact): MappedInputFactory =
      streamFrom(entity, maybePrefix = None, maybeUsedIn = None)

    def streamFrom(entity:      Entity with Artifact,
                   maybePrefix: Option[Prefix],
                   maybeUsedIn: Option[Step]
    ): MappedInputFactory =
      runPlan =>
        new EntityCommandParameter(maybePrefix, runPlan, entity) with Input with InputMapping {
          protected override val identifier: String       = StdIn.name.value
          override val maybeStep:            Option[Step] = maybeUsedIn
          override val mappedTo:             IOStream.In  = StdIn
        }

    def withoutPositionFrom(entity: Entity with Artifact): NoPositionInputFactory =
      withoutPositionFrom(entity, maybePrefix = None, maybeUsedIn = None)

    def withoutPositionFrom(entity:      Entity with Artifact,
                            maybePrefix: Option[Prefix],
                            maybeUsedIn: Option[Step]
    ): NoPositionInputFactory =
      runPlan =>
        new EntityCommandParameter(maybePrefix, runPlan, entity) with Input {
          protected override val identifier: String       = randomUUID().toString
          override val maybeStep:            Option[Step] = maybeUsedIn
        }

    def factory(entityFactory: Activity => Entity with Artifact,
                maybePrefix:   Option[Prefix] = None
    ): ActivityPositionInputFactory =
      activity =>
        positionArg =>
          runPlan =>
            new EntityCommandParameter(maybePrefix, runPlan, entityFactory(activity)) with Input with PositionInfo {
              protected override val identifier: String       = positionArg.toString
              override val maybeStep:            Option[Step] = None
              override val position:             Position     = positionArg
            }

    private implicit def converter(implicit
        renkuBaseUrl:  RenkuBaseUrl,
        fusekiBaseUrl: FusekiBaseUrl
    ): PartialEntityConverter[CommandParameter with Input] =
      new PartialEntityConverter[CommandParameter with Input] {
        override def convert[T <: CommandParameter with Input]: T => Either[Exception, PartialEntity] = {
          case input: EntityCommandParameter with Input =>
            PartialEntity(
              EntityTypes of renku / "CommandInput",
              rdfs / "label"     -> s"""Command Input "${input.value}"""".asJsonLD,
              renku / "consumes" -> input.entity.asJsonLD
            ).asRight
          case other => throw new IllegalStateException(s"$other not supported")
        }

        override lazy val toEntityId: CommandParameter with Input => Option[EntityId] = input =>
          input.runPlan.getEntityId map { runPlanId =>
            input.maybeStep match {
              case None       => runPlanId / "inputs" / input
              case Some(step) => runPlanId / "steps" / step / "inputs" / input
            }
          }
      }

    implicit def inputEncoder(implicit
        renkuBaseUrl:  RenkuBaseUrl,
        fusekiBaseUrl: FusekiBaseUrl
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

    val outputFolderCreation: FolderCreation
    val maybeProducedByStep:  Option[Step]
    protected val identifier: String
    override lazy val toString: String = s"output_$identifier"
  }

  object Output {

    sealed trait OutputFactory
    object OutputFactory {
      trait PositionOutputFactory
          extends OutputFactory
          with (Activity => Position => Entity with RunPlan => CommandParameter with Output with PositionInfo)
      trait MappedOutputFactory
          extends OutputFactory
          with (Activity => Entity with RunPlan => CommandParameter with Output with OutputMapping)
      trait NoPositionOutputFactory
          extends OutputFactory
          with (Activity => Entity with RunPlan => CommandParameter with Output)
    }

    def factory(entityFactory: Activity => Entity with Artifact): PositionOutputFactory =
      factory(entityFactory, maybePrefix = None, folderCreation = no, maybeProducedBy = None)

    def factory(entityFactory: Activity => Entity with Artifact, producedBy: Step): PositionOutputFactory =
      factory(entityFactory, maybePrefix = None, folderCreation = no, maybeProducedBy = producedBy.some)

    def factory(
        entityFactory:   Activity => Entity with Artifact,
        maybePrefix:     Option[Prefix],
        folderCreation:  FolderCreation,
        maybeProducedBy: Option[Step]
    ): PositionOutputFactory =
      activity =>
        positionArg =>
          runPlan =>
            new EntityCommandParameter(maybePrefix, runPlan, entityFactory(activity)) with Output with PositionInfo {
              override val outputFolderCreation: FolderCreation = folderCreation
              override val maybeProducedByStep:  Option[Step]   = maybeProducedBy
              protected override val identifier: String         = positionArg.toString
              override val position:             Position       = positionArg
            }

    def streamFactory(
        entityFactory: Activity => Entity with Artifact,
        to:            IOStream.Out
    ): MappedOutputFactory =
      streamFactory(entityFactory, maybePrefix = None, folderCreation = no, maybeProducedBy = None, to)

    def streamFactory(
        entityFactory:   Activity => Entity with Artifact,
        maybePrefix:     Option[Prefix],
        folderCreation:  FolderCreation,
        maybeProducedBy: Option[Step],
        to:              IOStream.Out
    ): MappedOutputFactory =
      activity =>
        runPlan =>
          new EntityCommandParameter(maybePrefix, runPlan, entityFactory(activity)) with Output with OutputMapping {
            override val outputFolderCreation: FolderCreation = folderCreation
            override val maybeProducedByStep:  Option[Step]   = maybeProducedBy
            protected override val identifier: String         = to.name.value
            override val mappedTo:             IOStream.Out   = to
          }

    def withoutPositionFactory(entityFactory: Activity => Entity with Artifact): NoPositionOutputFactory =
      withoutPositionFactory(entityFactory, maybePrefix = None, folderCreation = no, maybeProducedBy = None)

    def withoutPositionFactory(
        entityFactory:   Activity => Entity with Artifact,
        maybePrefix:     Option[Prefix],
        folderCreation:  FolderCreation,
        maybeProducedBy: Option[Step]
    ): NoPositionOutputFactory =
      activity =>
        runPlan =>
          new EntityCommandParameter(maybePrefix, runPlan, entityFactory(activity)) with Output {
            override val outputFolderCreation: FolderCreation = folderCreation
            override val maybeProducedByStep:  Option[Step]   = maybeProducedBy
            protected override val identifier: String         = randomUUID().toString
          }

    private implicit def converter(implicit
        renkuBaseUrl:  RenkuBaseUrl,
        fusekiBaseUrl: FusekiBaseUrl
    ): PartialEntityConverter[CommandParameter with Output] =
      new PartialEntityConverter[CommandParameter with Output] {
        override def convert[T <: CommandParameter with Output]: T => Either[Exception, PartialEntity] = {
          case output: EntityCommandParameter with Output =>
            PartialEntity(
              EntityTypes of renku / "CommandOutput",
              rdfs / "label"         -> s"""Command Output "${output.value}"""".asJsonLD,
              renku / "createFolder" -> output.outputFolderCreation.asJsonLD,
              renku / "produces"     -> output.entity.asJsonLD
            ).asRight
          case other => throw new IllegalStateException(s"$other not supported")
        }

        override lazy val toEntityId: CommandParameter with Output => Option[EntityId] =
          output =>
            output.runPlan.getEntityId map { runPlanId =>
              output.maybeProducedByStep match {
                case None       => runPlanId / "outputs" / output
                case Some(step) => runPlanId / "steps" / step / "outputs" / output
              }
            }
      }

    implicit def outputEncoder(implicit
        renkuBaseUrl:  RenkuBaseUrl,
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
