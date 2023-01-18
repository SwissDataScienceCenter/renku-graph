package io.renku.cli.model

import cats.syntax.either._
import io.renku.cli.model.Ontologies.{Prov, Renku}
import io.renku.graph.model.entityModel._
import io.renku.graph.model.generations
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.syntax._
import io.renku.jsonld.{Cursor, EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliEntity(
    resourceId:            ResourceId,
    location:              Location,
    checksum:              Checksum,
    generationResourceIds: List[generations.ResourceId]
)

object CliEntity {

  private val fileEntityTypes:   EntityTypes = EntityTypes.of(Prov.Entity)
  private val folderEntityTypes: EntityTypes = EntityTypes.of(Prov.Entity, Prov.Collection)

  private val withStrictEntityTypes: Cursor => Result[Boolean] =
    _.getEntityTypes.map(types => types == fileEntityTypes || types == folderEntityTypes)

  private def locationDecoder(entityTypes: EntityTypes): JsonLDDecoder[Location] =
    JsonLDDecoder.decodeString.emap { value =>
      entityTypes match {
        case types if types contains folderEntityTypes => Location.Folder.from(value).leftMap(_.getMessage)
        case types if types contains fileEntityTypes   => Location.File.from(value).leftMap(_.getMessage)
        case types                                     => s"Entity with unknown $types types".asLeft
      }
    }

  implicit def jsonLdDecoder: JsonLDDecoder[CliEntity] =
    JsonLDDecoder.cacheableEntity(fileEntityTypes, withStrictEntityTypes) { cursor =>
      for {
        resourceId         <- cursor.downEntityId.as[ResourceId]
        entityTypes        <- cursor.getEntityTypes
        location           <- cursor.downField(Prov.atLocation).as[Location](locationDecoder(entityTypes))
        checksum           <- cursor.downField(Renku.checksum).as[Checksum]
        maybeGenerationIds <- cursor.downField(Prov.qualifiedGeneration).as[List[generations.ResourceId]]
      } yield CliEntity(resourceId, location, checksum, maybeGenerationIds)
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliEntity] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.resourceId.asEntityId,
        entity.location.fold(_ => folderEntityTypes, _ => fileEntityTypes),
        Prov.atLocation          -> entity.location.asJsonLD,
        Renku.checksum           -> entity.checksum.asJsonLD,
        Prov.qualifiedGeneration -> entity.generationResourceIds.map(_.asEntityId).asJsonLD
      )
    }
}
