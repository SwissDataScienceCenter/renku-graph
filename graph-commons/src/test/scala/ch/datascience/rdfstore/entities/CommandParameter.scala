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

import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory.{ActivityPositionInputFactory, PositionInputFactory}
import ch.datascience.rdfstore.entities.CommandParameter.Output.FolderCreation.no
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLDEncoder}

import scala.language.postfixOps

sealed abstract class CommandParameter(val position:    Position,
                                       val maybePrefix: Option[Prefix],
                                       val runPlan:     Entity with RunPlan) {
  val value: Value
}

object CommandParameter {

  sealed abstract class EntityCommandParameter(override val position:    Position,
                                               override val maybePrefix: Option[Prefix],
                                               override val runPlan:     Entity with RunPlan,
                                               val entity:               Entity with Artifact)
      extends CommandParameter(position, maybePrefix, runPlan) {
    override lazy val value: Value = Value(entity.location)
  }

  final class Value private (val value: String) extends AnyVal with StringTinyType
  implicit object Value extends TinyTypeFactory[Value](new Value(_)) with NonBlank {
    def apply(location: Location): Value = Value(location.value)
  }

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank

  final class Position private (val value: Int) extends AnyVal with IntTinyType
  implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt

  private[entities] implicit val converter: PartialEntityConverter[CommandParameter] =
    new PartialEntityConverter[CommandParameter] {
      override def convert[T <: CommandParameter]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of renku / "CommandParameter",
            renku / "prefix"   -> entity.maybePrefix.asJsonLD,
            renku / "position" -> entity.position.asJsonLD
          ).asRight

      override lazy val toEntityId: CommandParameter => Option[EntityId] = _ => None
    }

  final class Argument(override val position:    Position,
                       override val maybePrefix: Option[Prefix],
                       override val runPlan:     Entity with RunPlan)
      extends CommandParameter(position, maybePrefix, runPlan) {
    override val value:         Value  = Value("input_path")
    override lazy val toString: String = s"argument_$position"
  }

  object Argument {

    trait ArgumentFactory extends (Position => Entity with RunPlan => CommandParameter with Argument)

    def factory(maybePrefix: Option[Prefix] = None): ArgumentFactory =
      position => runPlan => new Argument(position, maybePrefix, runPlan)

    private implicit def converter(
        implicit renkuBaseUrl: RenkuBaseUrl,
        fusekiBaseUrl:         FusekiBaseUrl
    ): PartialEntityConverter[CommandParameter with Argument] =
      new PartialEntityConverter[CommandParameter with Argument] {

        override def convert[T <: CommandParameter with Argument]: T => Either[Exception, PartialEntity] =
          argument =>
            PartialEntity(
              EntityTypes of renku / "CommandArgument",
              rdfs / "label" -> s"""Command Argument "${argument.value}"""".asJsonLD
            ).asRight

        override lazy val toEntityId: CommandParameter with Argument => Option[EntityId] = input =>
          input.runPlan.getEntityId map (runPlanId => EntityId.of(s"$runPlanId/arguments/$input"))
      }

    implicit def argumentEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                                 fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[CommandParameter with Argument] =
      JsonLDEncoder.instance[CommandParameter with Argument] { entity =>
        entity
          .asPartialJsonLD[CommandParameter]
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
          with (Activity => Position => Entity with RunPlan => EntityCommandParameter with Input)
      trait PositionInputFactory
          extends InputFactory
          with (Position => Entity with RunPlan => EntityCommandParameter with Input)
    }

    def from(entity: Entity with Artifact): PositionInputFactory =
      from(entity, maybePrefix = None, maybeUsedIn = None)

    def from(entity: Entity with Artifact, usedIn: Step): PositionInputFactory =
      from(entity, maybePrefix = None, maybeUsedIn = usedIn.some)

    def from(entity:      Entity with Artifact,
             maybePrefix: Option[Prefix],
             maybeUsedIn: Option[Step]): PositionInputFactory =
      position =>
        runPlan =>
          new EntityCommandParameter(position, maybePrefix, runPlan, entity) with Input {
            protected override val identifier: String       = position.toString
            override val maybeStep:            Option[Step] = maybeUsedIn
          }

    def factory(entityFactory: Activity => Entity with Artifact,
                maybePrefix:   Option[Prefix] = None): ActivityPositionInputFactory =
      activity =>
        position =>
          runPlan =>
            new EntityCommandParameter(position, maybePrefix, runPlan, entityFactory(activity)) with Input {
              protected override val identifier: String       = position.toString
              override val maybeStep:            Option[Step] = None
            }

    private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                   fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[CommandParameter with Input] =
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
              case None       => EntityId.of(s"$runPlanId/inputs/$input")
              case Some(step) => EntityId.of(s"$runPlanId/steps/$step/inputs/$input")
            }
          }
      }

    implicit def inputEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                              fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[CommandParameter with Input] =
      JsonLDEncoder.instance[CommandParameter with Input] { entity =>
        entity.asPartialJsonLD[CommandParameter] combine entity.asPartialJsonLD[CommandParameter with Input] getOrFail
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

    trait OutputFactory extends (Activity => Position => Entity with RunPlan => CommandParameter with Output)

    def factory(entityFactory: Activity => Entity with Artifact): OutputFactory =
      factory(entityFactory, maybePrefix = None, folderCreation = no, maybeProducedBy = None)

    def factory(entityFactory: Activity => Entity with Artifact, producedBy: Step): OutputFactory =
      factory(entityFactory, maybePrefix = None, folderCreation = no, maybeProducedBy = producedBy.some)

    def factory(
        entityFactory:   Activity => Entity with Artifact,
        maybePrefix:     Option[Prefix],
        folderCreation:  FolderCreation,
        maybeProducedBy: Option[Step]
    ): OutputFactory =
      activity =>
        position =>
          runPlan =>
            new EntityCommandParameter(position, maybePrefix, runPlan, entityFactory(activity)) with Output {
              override val outputFolderCreation: FolderCreation = folderCreation
              override val maybeProducedByStep:  Option[Step]   = maybeProducedBy
              protected override val identifier: String         = position.toString
            }

    private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                   fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[CommandParameter with Output] =
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
                case None       => EntityId.of(s"$runPlanId/outputs/$output")
                case Some(step) => EntityId.of(s"$runPlanId/steps/$step/outputs/$output")
              }
            }
      }

    implicit def outputEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                               fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[CommandParameter with Output] =
      JsonLDEncoder.instance { entity =>
        entity.asPartialJsonLD[CommandParameter] combine entity.asPartialJsonLD[CommandParameter with Output] getOrFail
      }

    final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
    implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_)) {
      val no:  FolderCreation = FolderCreation(false)
      val yes: FolderCreation = FolderCreation(true)
    }
  }
}
