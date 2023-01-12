package io.renku.graph.model.cli

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.datasets._
import io.renku.graph.model.images.Image
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliDataset(
    resourceId:         ResourceId,
    identifier:         Identifier,
    title:              Title,
    name:               Name,
    createdOrPublished: Date,
    creators:           NonEmptyList[CliPerson],
    description:        Option[Description],
    keywords:           List[Keyword],
    images:             List[Image],
    license:            Option[License],
    version:            Option[Version],
    datasetFiles:       List[CliDatasetFile],
    dateModified:       Option[DateModified],
    sameAs:             Option[CliDatasetSameAs],
    derivedFrom:        Option[DerivedFrom],
    originalIdentifier: Option[OriginalIdentifier],
    invalidationTime:   Option[InvalidationTime]
) {

  val provenance: CliDatasetProvenance =
    CliDatasetProvenance(createdOrPublished, dateModified, sameAs, derivedFrom, originalIdentifier, invalidationTime)

  def originalIdEqualCurrentId: Boolean =
    originalIdentifier.isEmpty || originalIdentifier.exists(_.value == identifier.value)

  def originalIdNotEqualCurrentId: Boolean =
    !originalIdEqualCurrentId
}

object CliDataset {

  private val entityTypes: EntityTypes = EntityTypes.of(schema / "Dataset", prov / "Entity")

  implicit def jsonLDDecoder(implicit
      fileDecoder:   JsonLDDecoder[CliDatasetFile],
      personDecoder: JsonLDDecoder[CliPerson]
  ): JsonLDDecoder[CliDataset] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        identifier <- cursor.downField(schema / "identifier").as[Identifier]
        title      <- cursor.downField(schema / "name").as[Title]
        name       <- cursor.downField(renku / "slug").as[Name]

        creators           <- cursor.downField(schema / "creator").as[NonEmptyList[CliPerson]]
        maybeDateCreated   <- cursor.downField(schema / "dateCreated").as[Option[DateCreated]]
        maybeDatePublished <- cursor.downField(schema / "datePublished").as[Option[DatePublished]]
        date <- maybeDateCreated
                  .orElse(maybeDatePublished)
                  .toRight(DecodingFailure("No dateCreated or datePublished found on dataset", Nil))
        maybeDateModified <- cursor.downField(schema / "dateModified").as[Option[DateModified]]
        maybeSameAs <- cursor
                         .downField(schema / "sameAs")
                         .as[CliDatasetSameAs]
                         .map(Option.apply)
                         .leftFlatMap(_ => Option.empty[CliDatasetSameAs].asRight)
        maybeDerivedFrom <-
          cursor
            .downField(prov / "wasDerivedFrom")
            .as[Option[DerivedFrom]](JsonLDDecoder.decodeOption(DerivedFrom.jsonLDDecoder))
        maybeOriginalIdentifier <- cursor.downField(renku / "originalIdentifier").as[Option[OriginalIdentifier]]
        maybeInvalidationTime   <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]

        maybeDescription <- cursor.downField(schema / "description").as[Option[Description]]
        keywords         <- cursor.downField(schema / "keywords").as[List[Option[Keyword]]].map(_.flatten).map(_.sorted)
        images           <- cursor.downField(schema / "image").as[List[Image]].map(_.sortBy(_.position))
        maybeLicense     <- cursor.downField(schema / "license").as[Option[License]]
        maybeVersion     <- cursor.downField(schema / "version").as[Option[Version]]

        parts <- cursor.downField(schema / "hasPart").as[List[CliDatasetFile]]

      } yield CliDataset(
        resourceId,
        identifier,
        title,
        name,
        date,
        creators,
        maybeDescription,
        keywords,
        images,
        maybeLicense,
        maybeVersion,
        parts,
        maybeDateModified,
        maybeSameAs,
        maybeDerivedFrom,
        maybeOriginalIdentifier,
        maybeInvalidationTime
      )
    }

  implicit def jsonLDEncoder(implicit
      fileEncoder:   JsonLDEncoder[CliDatasetFile],
      personEncoder: JsonLDEncoder[CliPerson]
  ): JsonLDEncoder[CliDataset] =
    JsonLDEncoder.instance { ds =>
      JsonLD.entity(
        ds.resourceId.asEntityId,
        entityTypes,
        List(
          Some(schema / "identifier" -> ds.identifier.asJsonLD),
          Some(schema / "name"       -> ds.title.asJsonLD),
          Some(renku / "slug"        -> ds.name.asJsonLD),
          Some(schema / "creator"    -> ds.creators.asJsonLD),
          ds.createdOrPublished match {
            case d: DateCreated   => (schema / "dateCreated"   -> d.asJsonLD).some
            case d: DatePublished => (schema / "datePublished" -> d.asJsonLD).some
          },
          ds.dateModified.map(m => schema / "dateModified" -> m.asJsonLD),
          ds.sameAs.map(e => schema / "sameAs" -> e.asJsonLD),
          ds.derivedFrom.map(e => prov / "wasDerivedFrom" -> e.asJsonLD),
          ds.originalIdentifier.map(e => renku / "originalIdentifier" -> e.asJsonLD),
          ds.invalidationTime.map(e => prov / "invalidatedAtTime" -> e.asJsonLD),
          ds.description.map(d => schema / "description" -> d.asJsonLD),
          Some(schema / "keywords" -> ds.keywords.asJsonLD),
          Some(schema / "image"    -> ds.images.asJsonLD),
          ds.license.map(e => schema / "license" -> e.asJsonLD),
          ds.version.map(e => schema / "version" -> e.asJsonLD),
          Some(schema / "hasPart" -> ds.datasetFiles.asJsonLD)
        ).flatten.toMap
      )
    }
}
