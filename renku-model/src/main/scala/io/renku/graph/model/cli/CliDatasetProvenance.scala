package io.renku.graph.model.cli

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model.{GitLabApiUrl, GraphClass, InvalidationTime, RenkuUrl}
import io.renku.graph.model.datasets._
import io.renku.graph.model.entities.Dataset.Identification
import io.renku.graph.model.entities.Person
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder, Property, Reverse}

/** Raw data as returned by the CLI.
 */
final case class CliDatasetProvenance(
    id:                 Identification,
    creators:           NonEmptyList[Person],
    createdAt:          Option[DateCreated],
    publishedAt:        Option[DatePublished],
    modifiedAt:         Option[DateModified],
    sameAs:             Option[CliDatasetSameAs],
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

  def toJsonLDProperties(cliProv: CliDatasetProvenance)(implicit gitLabApiUrl: GitLabApiUrl): Map[Property, JsonLD] = {
    implicit val personEncoder: JsonLDEncoder[Person] = Person.encoder(gitLabApiUrl, GraphClass.Persons)
    List(
      Some(schema / "creator" -> cliProv.creators.asJsonLD),
      cliProv.createdAt.map(d => schema / "dateCreated" -> d.asJsonLD),
      cliProv.publishedAt.map(d => schema / "datePublished" -> d.asJsonLD),
      cliProv.modifiedAt.map(d => schema / "dateModified" -> d.asJsonLD),
      cliProv.sameAs.map(e => schema / "sameAs" -> e.asJsonLD),
      cliProv.derivedFrom.map(e => prov / "wasDerivedFrom" -> e.asJsonLD),
      cliProv.originalIdentifier.map(e => renku / "originalIdentifier" -> e.asJsonLD),
      cliProv.invalidationTime.map(e => prov / "invalidatedAtTime" -> e.asJsonLD)
    ).flatten.toMap
  }

  implicit def jsonLDEncoder(implicit gitLabUrl: GitLabApiUrl): JsonLDEncoder[CliDatasetProvenance] =
    JsonLDEncoder.instance { data =>
      JsonLD.entity(data.id.resourceId.asEntityId, entityTypes, Reverse.empty, toJsonLDProperties(data))
    }

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
        result = CliDatasetProvenance(
                   identification,
                   creators,
                   maybeDateCreated,
                   maybeDatePublished,
                   maybeDateModified,
                   maybeSameAs,
                   maybeDerivedFrom,
                   maybeOriginalIdentifier,
                   maybeInvalidationTime
                 )
      } yield result
    }
}
