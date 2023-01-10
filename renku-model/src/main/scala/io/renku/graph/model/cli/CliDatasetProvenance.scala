package io.renku.graph.model.cli

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.{InvalidationTime, RenkuUrl}
import io.renku.graph.model.datasets._
import io.renku.graph.model.entities.Dataset.Identification
import io.renku.graph.model.entities.Person
import io.renku.jsonld.{EntityTypes, JsonLDDecoder}

/** Raw data as returned by the CLI.
 */
final case class CliDatasetProvenance(
    id:                 Identification,
    creators:           NonEmptyList[Person],
    createdAt:          Option[DateCreated],
    publishedAt:        Option[DatePublished],
    modifiedAt:         Option[DateModified],
    internalSameAs:     Option[InternalSameAs],
    externalSameAs:     Option[ExternalSameAs],
    derivedFrom:        Option[DerivedFrom],
    originalIdentifier: Option[OriginalIdentifier],
    invalidationTime:   Option[InvalidationTime]
) {

  def originalIdEqualCurrentId: Boolean =
    originalIdentifier.isEmpty || originalIdentifier.exists(_.value == id.identifier.value)

  def originalIdNotEqualCurrentId: Boolean =
    !originalIdEqualCurrentId
}

object CliDatasetProvenance {

  val entityTypes: EntityTypes = EntityTypes.of(schema / "Dataset", prov / "Entity")

  def decoder(
      identification:  Identification
  )(implicit renkuUrl: RenkuUrl): JsonLDDecoder[CliDatasetProvenance] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._

      def failIfNoCreators(creators: List[Person]) = Either.fromOption(
        NonEmptyList.fromList(creators),
        DecodingFailure(s"No creators on dataset with id: ${identification.identifier}", Nil)
      )

      for {
        creators           <- cursor.downField(schema / "creator").as[List[Person]] >>= failIfNoCreators
        maybeDateCreated   <- cursor.downField(schema / "dateCreated").as[Option[DateCreated]]
        maybeDatePublished <- cursor.downField(schema / "datePublished").as[Option[DatePublished]]
        maybeDateModified  <- cursor.downField(schema / "dateModified").as[Option[DateModified]]
        maybeInternalSameAs <- cursor
                                 .downField(schema / "sameAs")
                                 .as[InternalSameAs]
                                 .map(Option.apply)
                                 .leftFlatMap(_ => Option.empty[InternalSameAs].asRight)
        maybeExternalSameAs <- cursor
                                 .downField(schema / "sameAs")
                                 .as[ExternalSameAs]
                                 .map(Option.apply)
                                 .leftFlatMap(_ => Option.empty[ExternalSameAs].asRight)
        maybeDerivedFrom <-
          cursor
            .downField(prov / "wasDerivedFrom")
            .as[Option[DerivedFrom]](JsonLDDecoder.decodeOption(DerivedFrom.jsonLDDecoder))
        maybeOriginalIdentifier <- cursor.downField(renku / "originalIdentifier").as[Option[OriginalIdentifier]]
        maybeInvalidationTime   <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
        result = CliDatasetProvenance(
                   identification,
                   creators,
                   maybeDateCreated,
                   maybeDatePublished,
                   maybeDateModified,
                   maybeInternalSameAs,
                   maybeExternalSameAs,
                   maybeDerivedFrom,
                   maybeOriginalIdentifier,
                   maybeInvalidationTime
                 )
      } yield result
    }
}
