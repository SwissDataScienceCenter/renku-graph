package io.renku.cli.model

import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.usages._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder}
import io.renku.jsonld.syntax._

final case class CliUsage(resourceId: ResourceId, entity: CliEntity)

object CliUsage {

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Usage)

  implicit lazy val decoder: JsonLDDecoder[CliUsage] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        entity     <- cursor.downField(Prov.entity).as[CliEntity]
      } yield CliUsage(resourceId, entity)
    }

  implicit lazy val jsonLDEncoder: FlatJsonLDEncoder[CliUsage] =
    FlatJsonLDEncoder.unsafe { case usage =>
      JsonLD.entity(
        usage.resourceId.asEntityId,
        entityTypes,
        Prov.entity -> usage.entity.asJsonLD
      )
    }
}
