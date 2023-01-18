package io.renku.cli.model

import io.renku.cli.model.Ontologies.{Renku, Schema}
import io.renku.graph.model.commandParameters
import io.renku.graph.model.parameterValues._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliParameterValue(
    id:        ResourceId,
    parameter: commandParameters.ResourceId,
    value:     String
)

object CliParameterValue {
  private val entityTypes = EntityTypes.of(Schema.PropertyValue, Renku.ParameterValue)

  implicit val jsonLDDecoder: JsonLDDecoder[CliParameterValue] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        valueReferenceId <- cursor.downField(Schema.valueReference).downEntityId.as[commandParameters.ResourceId]
        parameterValue   <- cursor.downField(Schema.value).as[String]
      } yield CliParameterValue(resourceId, valueReferenceId, parameterValue)
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliParameterValue] =
    JsonLDEncoder.instance { pv =>
      JsonLD.entity(
        pv.id.asEntityId,
        entityTypes,
        Schema.valueReference -> pv.parameter.asJsonLD,
        Schema.value          -> pv.value.asJsonLD
      )
    }
}
