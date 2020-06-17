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
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLDEncoder}

import scala.language.postfixOps

sealed abstract class CommandParameter(val position: Position, val maybePrefix: Option[Prefix], val value: Value) {
  val entityId: EntityId = EntityId.blank
}

object CommandParameter {

  final class Value private (val value: String) extends AnyVal with StringTinyType
  implicit object Value extends TinyTypeFactory[Value](new Value(_)) with NonBlank

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank

  final class Position private (val value: Int) extends AnyVal with IntTinyType
  implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt

  private[entities] implicit val converter: PartialEntityConverter[CommandParameter] =
    new PartialEntityConverter[CommandParameter] {
      override def convert[T <: CommandParameter]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            entity.entityId.some,
            EntityTypes of (prov / "Entity", renku / "CommandParameter"),
            renku / "prefix"   -> entity.maybePrefix.asJsonLD,
            renku / "position" -> entity.position.asJsonLD
          ).asRight
    }

}

sealed trait Input {
  self: CommandParameter =>
  def inputValue:    Value
  def inputConsumes: List[Entity with Artifact]
  override lazy val toString: String = s"CommandInput${entityId.value}"
}

object Input {

  def apply(position:    Position,
            value:       Value,
            maybePrefix: Option[Prefix],
            consumes:    List[Entity with Artifact]): CommandParameter with Input =
    new CommandParameter(position, maybePrefix, value) with Input {
      override val inputValue:    Value                      = value
      override val inputConsumes: List[Entity with Artifact] = consumes
    }

  private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                 fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Input] =
    new PartialEntityConverter[Input] {
      override def convert[T <: Input]: T => Either[Exception, PartialEntity] = entity => {
        PartialEntity(
          None,
          EntityTypes of (renku / "CommandInput"),
          rdfs / "label"     -> s"""Command Input "${entity.inputValue}"""".asJsonLD,
          renku / "consumes" -> entity.inputConsumes.asJsonLD
        ).asRight
      }
    }

  implicit def inputEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                            fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[CommandParameter with Input] =
    JsonLDEncoder.instance[CommandParameter with Input] { entity =>
      entity.asPartialJsonLD[CommandParameter] combine entity.asPartialJsonLD[Input] getOrFail
    }
}

sealed trait Output {
  self: CommandParameter =>

  import ch.datascience.rdfstore.entities.Output.FolderCreation
  def outputValue:          Value
  def outputFolderCreation: FolderCreation
  def outputProduces:       List[Entity with Artifact]

  override lazy val toString: String = s"CommandOutput${entityId.value}"
}

object Output {
  def apply(position:       Position,
            value:          Value,
            maybePrefix:    Option[Prefix],
            folderCreation: FolderCreation,
            produces:       List[Entity with Artifact]): CommandParameter with Output =
    new CommandParameter(position, maybePrefix, value) with Output {
      override val outputFolderCreation: FolderCreation             = folderCreation
      override val outputProduces:       List[Entity with Artifact] = produces
      override val outputValue:          Value                      = value
    }

  private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                 fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Output] =
    new PartialEntityConverter[Output] {
      override def convert[T <: Output]: T => Either[Exception, PartialEntity] = entity => {
        PartialEntity(
          None,
          EntityTypes of (renku / "CommandOutput"),
          rdfs / "label"         -> s"""Command Output "${entity.outputValue}"""".asJsonLD,
          renku / "createFolder" -> entity.outputFolderCreation.asJsonLD,
          renku / "produces"     -> entity.outputProduces.asJsonLD
        ).asRight
      }
    }

  implicit def outputEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                             fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[CommandParameter with Output] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[CommandParameter] combine entity.asPartialJsonLD[Output] getOrFail
    }

  final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_))
}
