package io.renku.cli.model

import io.renku.cli.model.Ontologies.{Renku, Schema}
import io.renku.graph.model.commandParameters._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliCommandParameter(
    resourceId:   ResourceId,
    name:         Name,
    description:  Option[Description],
    prefix:       Option[Prefix],
    position:     Option[Position],
    defaultValue: ParameterDefaultValue
) extends CliCommandParameterBase

object CliCommandParameter {
  private val entityTypes: EntityTypes = EntityTypes.of(Renku.CommandParameter, Renku.CommandParameterBase)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit val jsonLDDecoder: JsonLDDecoder[CliCommandParameter] = JsonLDDecoder.entity(entityTypes) { cursor =>
    for {
      resourceId       <- cursor.downEntityId.as[ResourceId]
      position         <- cursor.downField(Renku.position).as[Option[Position]]
      name             <- cursor.downField(Schema.name).as[Name]
      maybeDescription <- cursor.downField(Schema.description).as[Option[Description]]
      maybePrefix      <- cursor.downField(Renku.prefix).as[Option[Prefix]]
      defaultValue     <- cursor.downField(Schema.defaultValue).as[ParameterDefaultValue]
    } yield CliCommandParameter(resourceId, name, maybeDescription, maybePrefix, position, defaultValue)
  }

  implicit val jsonLDEncoder: JsonLDEncoder[CliCommandParameter] =
    JsonLDEncoder.instance { param =>
      JsonLD.entity(
        param.resourceId.asEntityId,
        entityTypes,
        Renku.position      -> param.position.asJsonLD,
        Schema.name         -> param.name.asJsonLD,
        Schema.description  -> param.description.asJsonLD,
        Renku.prefix        -> param.prefix.asJsonLD,
        Schema.defaultValue -> param.defaultValue.asJsonLD
      )
    }
}
