package io.renku.cli.model

import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.datasets._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder}

final case class CliDatasetFile(
    resourceId:       PartResourceId,
    external:         PartExternal,
    entity:           CliEntity,
    dateCreated:      DateCreated,
    source:           Option[PartSource],
    invalidationTime: Option[InvalidationTime]
)

object CliDatasetFile {
  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Entity, Schema.DigitalDocument)

  implicit def jsonLDDecoder: JsonLDDecoder[CliDatasetFile] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId            <- cursor.downEntityId.as[PartResourceId]
        external              <- cursor.downField(Renku.external).as[PartExternal]
        entity                <- cursor.downField(Prov.entity).as[CliEntity]
        dateCreated           <- cursor.downField(Schema.dateCreated).as[DateCreated]
        maybeSource           <- cursor.downField(Renku.source).as[Option[PartSource]]
        maybeInvalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
        part = CliDatasetFile(resourceId, external, entity, dateCreated, maybeSource, maybeInvalidationTime)
      } yield part
    }

  implicit def jsonLDEncoder: FlatJsonLDEncoder[CliDatasetFile] =
    FlatJsonLDEncoder.unsafe { file =>
      JsonLD.entity(
        file.resourceId.asEntityId,
        entityTypes,
        List(
          Some(Renku.external     -> file.external.asJsonLD),
          Some(Prov.entity        -> file.entity.asJsonLD),
          Some(Schema.dateCreated -> file.dateCreated.asJsonLD),
          file.source.map(s => Renku.source -> s.asJsonLD),
          file.invalidationTime.map(t => Prov.invalidatedAtTime -> t.asJsonLD)
        ).flatten.toMap
      )
    }
}
