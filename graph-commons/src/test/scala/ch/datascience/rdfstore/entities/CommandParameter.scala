package ch.datascience.rdfstore.entities

import ch.datascience.rdfstore.entities.CommandOutput.FolderCreation
import ch.datascience.rdfstore.entities.CommandParameter.{Position, Value}
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt}
import ch.datascience.tinytypes.{BooleanTinyType, IntTinyType, StringTinyType, TinyTypeFactory}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}

sealed trait CommandParameter {
  val position: Position
}

object CommandParameter {

  final class Value private (val value: String) extends AnyVal with StringTinyType
  implicit object Value extends TinyTypeFactory[Value](new Value(_)) with NonBlank

  final class Position private (val value: Int) extends AnyVal with IntTinyType
  implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt
}

final case class CommandInput(value: Value, position: Position, consumes: List[Artifact]) extends CommandParameter {
  val entityId:              EntityId = EntityId.blank
  lazy val entityIdToString: String   = s"CommandInput${entityId.value}"
}

object CommandInput {

  implicit def encoder(): JsonLDEncoder[CommandInput] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.entityId,
        EntityTypes of (prov / "Entity", renku / "CommandInput", renku / "CommandParameter"),
        rdfs / "label"     -> s"""Command Input "${entity.value}"""".asJsonLD,
        renku / "position" -> entity.position.asJsonLD,
        renku / "consumes" -> entity.consumes.asJsonLD
      )
    }
}

final case class CommandOutput(value:          Value,
                               position:       Position,
                               folderCreation: FolderCreation,
                               produces:       List[Artifact])
    extends CommandParameter {
  val entityId:              EntityId = EntityId.blank
  lazy val entityIdToString: String   = s"CommandOutput${entityId.value}"
}

object CommandOutput {

  implicit def encoder(): JsonLDEncoder[CommandOutput] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.entityId,
        EntityTypes of (prov / "Entity", renku / "CommandOutput", renku / "CommandParameter"),
        rdfs / "label"         -> s"""Command Output "${entity.value}"""".asJsonLD,
        renku / "position"     -> entity.position.asJsonLD,
        renku / "createFolder" -> entity.folderCreation.asJsonLD,
        renku / "produces"     -> entity.produces.asJsonLD
      )
    }

  final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_))
}
