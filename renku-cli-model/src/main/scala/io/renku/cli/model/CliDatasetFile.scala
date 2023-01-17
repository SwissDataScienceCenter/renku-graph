package io.renku.cli.model

import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.datasets._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliDatasetFile(
    resourceId:       PartResourceId,
    external:         PartExternal,
    entity:           CliEntity,
    dateCreated:      DateCreated,
    source:           Option[PartSource],
    invalidationTime: Option[InvalidationTime]
)

object CliDatasetFile {
  private val entityTypes: EntityTypes = EntityTypes.of(prov / "Entity", schema / "DigitalDocument")

  implicit def jsonLDDecoder: JsonLDDecoder[CliDatasetFile] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId            <- cursor.downEntityId.as[PartResourceId]
        external              <- cursor.downField(renku / "external").as[PartExternal]
        entity                <- cursor.downField(prov / "entity").as[CliEntity]
        dateCreated           <- cursor.downField(schema / "dateCreated").as[DateCreated]
        maybeSource           <- cursor.downField(renku / "source").as[Option[PartSource]]
        maybeInvalidationTime <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
        part = CliDatasetFile(resourceId, external, entity, dateCreated, maybeSource, maybeInvalidationTime)
      } yield part
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliDatasetFile] =
    JsonLDEncoder.instance { file =>
      JsonLD.entity(
        file.resourceId.asEntityId,
        entityTypes,
        List(
          Some(renku / "external"     -> file.external.asJsonLD),
          Some(prov / "entity"        -> file.entity.asJsonLD),
          Some(schema / "dateCreated" -> file.dateCreated.asJsonLD),
          file.source.map(s => renku / "source" -> s.asJsonLD),
          file.invalidationTime.map(t => prov / "invalidatedAtTime" -> t.asJsonLD)
        ).flatten.toMap
      )
    }
}
