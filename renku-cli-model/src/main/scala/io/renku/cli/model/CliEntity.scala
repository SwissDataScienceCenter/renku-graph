package io.renku.cli.model

import cats.syntax.either._
import io.renku.graph.model.Schemas.{prov, renku}
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

  private val fileEntityTypes:   EntityTypes = EntityTypes.of(prov / "Entity")
  private val folderEntityTypes: EntityTypes = EntityTypes.of(prov / "Entity", prov / "Collection")

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
        location           <- cursor.downField(prov / "atLocation").as[Location](locationDecoder(entityTypes))
        checksum           <- cursor.downField(renku / "checksum").as[Checksum]
        maybeGenerationIds <- cursor.downField(prov / "qualifiedGeneration").as[List[generations.ResourceId]]
      } yield CliEntity(resourceId, location, checksum, maybeGenerationIds)
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliEntity] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.resourceId.asEntityId,
        entity.location.fold(_ => folderEntityTypes, _ => fileEntityTypes),
        prov / "atLocation"          -> entity.location.asJsonLD,
        renku / "checksum"           -> entity.checksum.asJsonLD,
        prov / "qualifiedGeneration" -> entity.generationResourceIds.map(_.asEntityId).asJsonLD
      )
    }
}
