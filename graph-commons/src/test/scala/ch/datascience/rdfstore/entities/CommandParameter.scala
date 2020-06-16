package ch.datascience.rdfstore.entities

import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.Output.FolderCreation
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt}
import ch.datascience.tinytypes.{BooleanTinyType, IntTinyType, StringTinyType, TinyTypeFactory}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}

sealed abstract class CommandParameter(val position: Position, val prefix: Option[Prefix], val value: Value) {
  val entityId: EntityId = EntityId.blank
}

object CommandParameter {

  final class Value private (val value: String) extends AnyVal with StringTinyType
  implicit object Value extends TinyTypeFactory[Value](new Value(_)) with NonBlank

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank

  final class Position private (val value: Int) extends AnyVal with IntTinyType
  implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt

  implicit val encoder: JsonLDEncoder[CommandParameter] = {

    JsonLD.entity(
      entity.entityId,
      EntityTypes of (prov / "Entity", renku / "CommandParameter"),
//        rdfs / "label"     -> s"""Command Input "${entity.value}"""".asJsonLD,
      renku / "prefix"   -> entity.prefix.asJsonLD,
      renku / "position" -> entity.position.asJsonLD
    )
  }

  implicit val converter: CommandParameter => PartialEntity = ???

}

sealed trait Input {
  self: CommandParameter =>

  def inputConsumes: ArtifactEntityCollection
  override lazy val toString: String = s"CommandInput${entityId.value}"
}

object Input {

  def apply(position: Position,
            value:    Value,
            prefix:   Option[Prefix],
            consumes: ArtifactEntityCollection): CommandParameter with Input =
    new CommandParameter(position, prefix, value) with Input {
      override val inputConsumes: ArtifactEntityCollection = consumes
    }

  implicit def encoder(): JsonLDEncoder[Input] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.entityId,
        EntityTypes of (prov / "Entity", renku / "CommandInput", renku / "CommandParameter"),
        rdfs / "label"     -> s"""Command Input "${entity.value}"""".asJsonLD,
        renku / "prefix"   -> entity.prefix.asJsonLD,
        renku / "position" -> entity.position.asJsonLD,
        renku / "consumes" -> entity.consumes.asJsonLD
      )
    }
}

sealed trait Output {
  self: CommandParameter =>

  def outputFolderCreation: FolderCreation
  def outputProduces:       ArtifactEntityCollection

  lazy val entityIdToString: String = s"CommandOutput${entityId.value}"
}

object Output {

  def apply(position:       Position,
            value:          Value,
            prefix:         Option[Prefix],
            folderCreation: FolderCreation,
            produces:       ArtifactEntityCollection): CommandParameter with Output =
    new CommandParameter(position, prefix, value) with Output {
      override val outputFolderCreation: FolderCreation           = folderCreation
      override val outputProduces:       ArtifactEntityCollection = produces
    }

  implicit def encoder(): JsonLDEncoder[Output] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.entityId,
        EntityTypes of (prov / "Entity", renku / "CommandOutput", renku / "CommandParameter"),
        rdfs / "label"         -> s"""Command Output "${entity.value}"""".asJsonLD,
        renku / "prefix"       -> entity.prefix.asJsonLD,
        renku / "position"     -> entity.position.asJsonLD,
        renku / "createFolder" -> entity.folderCreation.asJsonLD,
        renku / "produces"     -> entity.produces.asJsonLD
      )
    }

  implicit val converter: Output => PartialEntity = ???

  implicit val outputEncoder: JsonLDEncoder[CommandParameter with Output] = {
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[CommandParameter] combine entity.asPartialJsonLD[Output] getOrElse new Exception(
        "Output cannot be converted to JsonLD"
      )
    }
  }

  final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_))
}
