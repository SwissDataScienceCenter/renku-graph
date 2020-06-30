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
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory.{ActivityPositionInput, PositionInput}
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLDEncoder}

import scala.language.postfixOps

sealed abstract class CommandParameter(val position: Position, val maybePrefix: Option[Prefix]) {
  val entityId: EntityId = EntityId.blank
}

object CommandParameter {

  sealed abstract class ActivityCommandParameter(override val position:    Position,
                                                 override val maybePrefix: Option[Prefix],
                                                 activity:                 => Activity,
                                                 entityFactory:            Activity => Entity with Artifact)
      extends CommandParameter(position, maybePrefix) {
    lazy val entity: Entity with Artifact = entityFactory(activity)
    lazy val value:  Value                = Value(entity.location)
  }

  sealed abstract class ValueCommandParameter(override val position:    Position,
                                              override val maybePrefix: Option[Prefix],
                                              val value:                Value)
      extends CommandParameter(position, maybePrefix)

  sealed abstract class EntityCommandParameter(override val position:    Position,
                                               override val maybePrefix: Option[Prefix],
                                               val entity:               Entity with Artifact)
      extends CommandParameter(position, maybePrefix) {
    lazy val value: Value = Value(entity.location)
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
            entity.entityId,
            EntityTypes of renku / "CommandParameter",
            renku / "prefix"   -> entity.maybePrefix.asJsonLD,
            renku / "position" -> entity.position.asJsonLD
          ).asRight

      override def toEntityId: CommandParameter => Option[EntityId] = entity => entity.entityId.some
    }

  sealed trait Input {
    self: CommandParameter =>
    override lazy val toString: String = "CommandInput"
  }

  object Input {

    sealed trait InputFactory[+T <: CommandParameter]

    object InputFactory {
      trait ActivityPositionInput[+T <: CommandParameter]
          extends InputFactory[T]
          with (Activity => Position => T with Input)
      trait PositionInput[+T <: CommandParameter] extends InputFactory[T] with (Position => T with Input)
    }

    def from(entity: Entity with Artifact, maybePrefix: Option[Prefix] = None): PositionInput[EntityCommandParameter] =
      position => new EntityCommandParameter(position, maybePrefix, entity) with Input

    def factory(entityFactory: Activity => Entity with Artifact,
                maybePrefix:   Option[Prefix] = None): ActivityPositionInput[EntityCommandParameter] =
      activity => position => new EntityCommandParameter(position, maybePrefix, entityFactory(activity)) with Input

    private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                   fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[CommandParameter with Input] =
      new PartialEntityConverter[CommandParameter with Input] {
        override def convert[T <: CommandParameter with Input]: T => Either[Exception, PartialEntity] = {
          case input: ValueCommandParameter with Input =>
            PartialEntity(
              EntityTypes of renku / "CommandInput",
              rdfs / "label" -> s"""Command Input "${input.value}"""".asJsonLD
            ).asRight
          case input: EntityCommandParameter with Input =>
            PartialEntity(
              EntityTypes of renku / "CommandInput",
              rdfs / "label"     -> s"""Command Input "${input.value}"""".asJsonLD,
              renku / "consumes" -> input.entity.asJsonLD
            ).asRight
          case other => throw new IllegalStateException(s"$other not supported")
        }
        override def toEntityId: CommandParameter with Input => Option[EntityId] =
          _ => None
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

    def outputFolderCreation: FolderCreation
    override lazy val toString: String = "CommandOutput"
  }

  object Output {

    def factory(
        entityFactory:  Activity => Entity with Artifact,
        maybePrefix:    Option[Prefix] = None,
        folderCreation: FolderCreation = FolderCreation(false)
    ): Activity => Position => CommandParameter with Output =
      activity =>
        position =>
          new ActivityCommandParameter(position, maybePrefix, activity, entityFactory) with Output {
            override val outputFolderCreation: FolderCreation = folderCreation
          }

    private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                   fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[CommandParameter with Output] =
      new PartialEntityConverter[CommandParameter with Output] {
        override def convert[T <: CommandParameter with Output]: T => Either[Exception, PartialEntity] = {
          case output: ValueCommandParameter with Output =>
            PartialEntity(
              EntityTypes of renku / "CommandOutput",
              rdfs / "label"         -> s"""Command Output "${output.value}"""".asJsonLD,
              renku / "createFolder" -> output.outputFolderCreation.asJsonLD
            ).asRight
          case output: ActivityCommandParameter with Output =>
            PartialEntity(
              EntityTypes of renku / "CommandOutput",
              rdfs / "label"         -> s"""Command Output "${output.value}"""".asJsonLD,
              renku / "createFolder" -> output.outputFolderCreation.asJsonLD,
              renku / "produces"     -> output.entity.asJsonLD
            ).asRight
          case other => throw new IllegalStateException(s"$other not supported")
        }
        override def toEntityId: CommandParameter with Output => Option[EntityId] =
          _ => None

      }

    implicit def outputEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                               fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[CommandParameter with Output] =
      JsonLDEncoder.instance { entity =>
        entity.asPartialJsonLD[CommandParameter] combine entity.asPartialJsonLD[CommandParameter with Output] getOrFail
      }

    final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
    implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_))
  }
}
