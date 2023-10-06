/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.cli.model

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.datasets._
import io.renku.graph.model.images.Image
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import monocle.{Lens, Traversal}

final case class CliDataset(
    resourceId:         ResourceId,
    identifier:         Identifier,
    name:               Name,
    slug:               Slug,
    createdOrPublished: CreatedOrPublished,
    dateModified:       DateModified,
    creators:           NonEmptyList[CliPerson],
    description:        Option[Description],
    keywords:           List[Keyword],
    images:             List[Image],
    license:            Option[License],
    version:            Option[Version],
    datasetFiles:       List[CliDatasetFile],
    sameAs:             Option[SameAs],
    derivedFrom:        Option[DerivedFrom],
    originalIdentifier: Option[OriginalIdentifier],
    invalidationTime:   Option[InvalidationTime],
    publicationEvents:  List[CliPublicationEvent]
) extends CliModel {

  val provenance: CliDatasetProvenance =
    CliDatasetProvenance(createdOrPublished, dateModified, sameAs, derivedFrom, originalIdentifier, invalidationTime)

  def originalIdEqualCurrentId: Boolean =
    originalIdentifier.isEmpty || originalIdentifier.exists(_.value == identifier.value)

  def originalIdNotEqualCurrentId: Boolean =
    !originalIdEqualCurrentId
}

object CliDataset {

  private[model] val entityTypes: EntityTypes = EntityTypes.of(Schema.Dataset, Prov.Entity)

  implicit def jsonLDDecoder(implicit
      fileDecoder:   JsonLDDecoder[CliDatasetFile],
      personDecoder: JsonLDDecoder[CliPerson]
  ): JsonLDDecoder[CliDataset] = JsonLDDecoder.entity(entityTypes) { cursor =>
    def failIfNoCreators(identifier: Identifier)(creators: List[CliPerson]) = Either.fromOption(
      NonEmptyList.fromList(creators),
      DecodingFailure(show"No creators on dataset with id: $identifier", Nil)
    )

    for {
      resourceId         <- cursor.downEntityId.as[ResourceId]
      identifier         <- cursor.downField(Schema.identifier).as[Identifier]
      name               <- cursor.downField(Schema.name).as[Name]
      slug               <- cursor.downField(Renku.slug).as[Slug]
      creators           <- cursor.downField(Schema.creator).as[List[CliPerson]] >>= failIfNoCreators(identifier)
      maybeDateCreated   <- cursor.downField(Schema.dateCreated).as[Option[DateCreated]]
      maybeDatePublished <- cursor.downField(Schema.datePublished).as[Option[DatePublished]]
      date <- maybeDateCreated
                .orElse(maybeDatePublished)
                .toRight(DecodingFailure("No dateCreated or datePublished found on dataset", Nil))
      maybeDateModified <- cursor.downField(Schema.dateModified).as[DateModified]
      maybeInternalSameAs <- cursor
                               .downField(Schema.sameAs)
                               .as[InternalSameAs]
                               .map(Option.apply)
                               .leftFlatMap(_ => Option.empty[InternalSameAs].asRight)
      maybeExternalSameAs <- cursor
                               .downField(Schema.sameAs)
                               .as[ExternalSameAs]
                               .map(Option.apply)
                               .leftFlatMap(_ => Option.empty[ExternalSameAs].asRight)
      maybeDerivedFrom <- cursor
                            .downField(Prov.wasDerivedFrom)
                            .as[Option[DerivedFrom]](JsonLDDecoder.decodeOption(DerivedFrom.jsonLDDecoder))
      maybeOriginalIdentifier <- cursor.downField(Renku.originalIdentifier).as[Option[OriginalIdentifier]]
      maybeInvalidationTime   <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]

      maybeDescription <- cursor.downField(Schema.description).as[Option[Description]]
      keywords         <- cursor.downField(Schema.keywords).as[List[Option[Keyword]]].map(_.flatten).map(_.sorted)
      images           <- cursor.downField(Schema.image).as[List[Image]].map(_.sortBy(_.position))
      maybeLicense     <- cursor.downField(Schema.license).as[Option[License]]
      maybeVersion     <- cursor.downField(Schema.version).as[Option[Version]]
      parts            <- cursor.downField(Schema.hasPart).as[List[CliDatasetFile]]
      publicationEvents <-
        cursor.focusTop.as(JsonLDDecoder.decodeList(CliPublicationEvent.decoder(identifier, resourceId)))
    } yield CliDataset(
      resourceId,
      identifier,
      name,
      slug,
      date,
      maybeDateModified,
      creators,
      maybeDescription,
      keywords,
      images,
      maybeLicense,
      maybeVersion,
      parts,
      maybeInternalSameAs orElse maybeExternalSameAs,
      maybeDerivedFrom,
      maybeOriginalIdentifier,
      maybeInvalidationTime,
      publicationEvents
    )
  }

  implicit def jsonLDEncoder(implicit
      fileEncoder:   JsonLDEncoder[CliDatasetFile],
      personEncoder: JsonLDEncoder[CliPerson]
  ): JsonLDEncoder[CliDataset] = JsonLDEncoder.instance { ds =>
    JsonLD.entity(
      ds.resourceId.asEntityId,
      entityTypes,
      List(
        Some(Schema.identifier -> ds.identifier.asJsonLD),
        Some(Schema.name       -> ds.name.asJsonLD),
        Some(Renku.slug        -> ds.slug.asJsonLD),
        Some(Schema.creator    -> ds.creators.asJsonLD),
        ds.createdOrPublished match {
          case d: DateCreated   => (Schema.dateCreated   -> d.asJsonLD).some
          case d: DatePublished => (Schema.datePublished -> d.asJsonLD).some
        },
        Some(Schema.dateModified -> ds.dateModified.asJsonLD),
        ds.sameAs.map(e => Schema.sameAs -> e.asJsonLD),
        ds.derivedFrom.map(e => Prov.wasDerivedFrom -> e.asJsonLD),
        ds.originalIdentifier.map(e => Renku.originalIdentifier -> e.asJsonLD),
        ds.invalidationTime.map(e => Prov.invalidatedAtTime -> e.asJsonLD),
        ds.description.map(d => Schema.description -> d.asJsonLD),
        Some(Schema.keywords -> ds.keywords.asJsonLD),
        Some(Schema.image    -> ds.images.asJsonLD),
        ds.license.map(e => Schema.license -> e.asJsonLD),
        ds.version.map(e => Schema.version -> e.asJsonLD),
        Some(Schema.hasPart -> ds.datasetFiles.asJsonLD)
      ).flatten.toMap
    )
  }

  /** Encodes into a flat array, including publication events. */
  def flatJsonLDEncoder(implicit
      fileEncoder:   JsonLDEncoder[CliDatasetFile],
      personEncoder: JsonLDEncoder[CliPerson]
  ): JsonLDEncoder[CliDataset] = JsonLDEncoder.instance { dataset =>
    val data = dataset.asNestedJsonLD :: dataset.publicationEvents.map(_.asNestedJsonLD)
    JsonLD.arr(data: _*).flatten.fold(throw _, identity)
  }

  object Lenses {
    val publicationEventList: Lens[CliDataset, List[CliPublicationEvent]] =
      Lens[CliDataset, List[CliPublicationEvent]](_.publicationEvents)(evs => _.copy(publicationEvents = evs))

    val publicationEvents: Traversal[CliDataset, CliPublicationEvent] =
      publicationEventList.andThen(Traversal.fromTraverse[List, CliPublicationEvent])
  }
}
