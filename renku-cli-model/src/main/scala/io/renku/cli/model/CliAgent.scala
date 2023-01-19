package io.renku.cli.model

import io.renku.graph.model.agents._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder}
import Ontologies.{Prov, Schema}

final case class CliAgent(
    resourceId: ResourceId,
    name:       Name
)

object CliAgent {
  private val entityTypes: EntityTypes = EntityTypes.of(Prov.SoftwareAgent)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit val jsonLDEncoder: FlatJsonLDEncoder[CliAgent] =
    FlatJsonLDEncoder.unsafe { agent =>
      JsonLD.entity(
        agent.resourceId.asEntityId,
        entityTypes,
        Schema.name -> agent.name.asJsonLD
      )
    }

  implicit val jsonLDDecoder: JsonLDDecoder[CliAgent] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        label      <- cursor.downField(Schema.name).as[Name]
      } yield CliAgent(resourceId, label)
    }

}
