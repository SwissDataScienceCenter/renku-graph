package io.renku.cli.model

import io.renku.cli.model.Ontologies.{Prov, Renku}
import io.renku.graph.model.entityModel._
import io.renku.graph.model.generations
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.syntax._
import io.renku.jsonld.{Cursor, EntityTypes, JsonLD, JsonLDDecoder}

final case class CliEntity(
    resourceId:    ResourceId,
    path:          EntityPath,
    checksum:      Checksum,
    generationIds: List[generations.ResourceId]
)

object CliEntity {

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Entity)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  private val withStrictEntityTypes: Cursor => Result[Boolean] =
    _.getEntityTypes.map(types => types == entityTypes)

  implicit def jsonLdDecoder: JsonLDDecoder[CliEntity] =
    JsonLDDecoder.cacheableEntity(entityTypes, withStrictEntityTypes) { cursor =>
      for {
        resourceId    <- cursor.downEntityId.as[ResourceId]
        path          <- cursor.downField(Prov.atLocation).as[EntityPath]
        checksum      <- cursor.downField(Renku.checksum).as[Checksum]
        generationIds <- cursor.downField(Prov.qualifiedGeneration).as[List[generations.ResourceId]]
      } yield CliEntity(resourceId, path, checksum, generationIds)
    }

  implicit def jsonLDEncoder: FlatJsonLDEncoder[CliEntity] =
    FlatJsonLDEncoder.unsafe { entity =>
      JsonLD.entity(
        entity.resourceId.asEntityId,
        entityTypes,
        Prov.atLocation          -> entity.path.asJsonLD,
        Renku.checksum           -> entity.checksum.asJsonLD,
        Prov.qualifiedGeneration -> entity.generationIds.asJsonLD
      )
    }
}
