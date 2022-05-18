/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.entities

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.datasets._
import io.renku.graph.model.entities.Dataset.Provenance._
import io.renku.graph.model.entities.Dataset._
import io.renku.graph.model.{GitLabApiUrl, InvalidationTime, RenkuBaseUrl}

import java.time.Instant

final case class Dataset[+P <: Provenance](identification:    Identification,
                                           provenance:        P,
                                           additionalInfo:    AdditionalInfo,
                                           parts:             List[DatasetPart],
                                           publicationEvents: List[PublicationEvent]
) extends DatasetOps[P]

object Dataset {

  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.JsonLDDecoder._
  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def from[P <: Provenance](identification:    Identification,
                            provenance:        P,
                            additionalInfo:    AdditionalInfo,
                            parts:             List[DatasetPart],
                            publicationEvents: List[PublicationEvent]
  ): ValidatedNel[String, Dataset[P]] = List(
    validateDates(parts, identification)(provenance),
    validate(publicationEvents, identification)
  ).sequence.map { _ =>
    Dataset(identification, provenance, additionalInfo, parts, publicationEvents)
  }

  private def validateDates[P <: Provenance](parts:          List[DatasetPart],
                                             identification: Identification
  ): P => ValidatedNel[String, Unit] = {
    case p: Provenance.Internal                         => verifyDate(parts, identification, p.date.value)
    case p: Provenance.ImportedExternal                 => verifyDate(parts, identification, p.date.instant)
    case p: Provenance.ImportedInternalAncestorExternal => verifyDate(parts, identification, p.date.instant)
    case p: Provenance.Modified                         => validate(p.maybeInvalidationTime, identification)(p)
    case _ => ().validNel[String]
  }

  private def verifyDate(parts:          List[DatasetPart],
                         identification: Identification,
                         datasetDate:    Instant
  ): ValidatedNel[String, Unit] = parts
    .map { part =>
      if ((part.dateCreated.value compareTo datasetDate) >= 0) ().validNel[String]
      else
        s"Dataset ${identification.identifier} Part ${part.entity.location} startTime ${part.dateCreated} is older than Dataset $datasetDate".invalidNel
    }
    .sequence
    .void

  private def validate[P <: Provenance](
      maybeInvalidationTime: Option[InvalidationTime],
      identification:        Identification
  ): P => ValidatedNel[String, Unit] = provenance =>
    maybeInvalidationTime match {
      case Some(time) if (time.value compareTo provenance.date.instant) < 0 =>
        s"Dataset ${identification.identifier} invalidationTime $time is older than Dataset ${provenance.date}".invalidNel
      case _ => ().validNel[String]
    }

  private def validate(
      publishingEvents: List[PublicationEvent],
      identification:   Identification
  ): ValidatedNel[String, Unit] = publishingEvents
    .map {
      case event if event.datasetResourceId == identification.resourceId => ().validNel[String]
      case event =>
        (show"PublicationEvent ${event.resourceId} refers to ${event.about} " +
          show"that points to ${event.datasetResourceId} but should be pointing to ${identification.resourceId}").invalidNel
    }
    .sequence
    .void

  final case class Identification(
      resourceId: ResourceId,
      identifier: Identifier,
      title:      Title,
      name:       Name
  )

  object Identification {
    private[Dataset] implicit lazy val encoder: Identification => Map[Property, JsonLD] = {
      case Identification(_, identifier, title, name) =>
        Map(
          schema / "identifier" -> identifier.asJsonLD,
          schema / "name"       -> title.asJsonLD,
          renku / "slug"        -> name.asJsonLD
        )
    }

    private[Dataset] implicit lazy val decoder: JsonLDDecoder[Identification] = JsonLDDecoder.entity(entityTypes) {
      cursor =>
        for {
          resourceId <- cursor.downEntityId.as[ResourceId]
          identifier <- cursor.downField(schema / "identifier").as[Identifier]
          title      <- cursor.downField(schema / "name").as[Title]
          name       <- cursor.downField(renku / "slug").as[Name]
        } yield Identification(resourceId, identifier, title, name)
    }
  }

  sealed trait Provenance extends Product with Serializable {
    type D <: Date
    val topmostSameAs:      TopmostSameAs
    val initialVersion:     InitialVersion
    val topmostDerivedFrom: TopmostDerivedFrom
    val date:               D
    val creators:           NonEmptyList[Person]
  }

  object Provenance {

    implicit object Internal
    final case class Internal(resourceId: ResourceId,
                              identifier: Identifier,
                              date:       DateCreated,
                              creators:   NonEmptyList[Person]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(resourceId.asEntityId)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    implicit object ImportedExternal
    final case class ImportedExternal(resourceId: ResourceId,
                                      identifier: Identifier,
                                      sameAs:     ExternalSameAs,
                                      date:       DatePublished,
                                      creators:   NonEmptyList[Person]
    ) extends Provenance {
      override type D = DatePublished
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(sameAs)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    implicit object ImportedInternal
    sealed trait ImportedInternal extends Provenance {
      val resourceId:     ResourceId
      val identifier:     Identifier
      val sameAs:         InternalSameAs
      val topmostSameAs:  TopmostSameAs
      val date:           D
      val initialVersion: InitialVersion
      val creators:       NonEmptyList[Person]

      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    final case class ImportedInternalAncestorExternal(resourceId:     ResourceId,
                                                      identifier:     Identifier,
                                                      sameAs:         InternalSameAs,
                                                      topmostSameAs:  TopmostSameAs,
                                                      initialVersion: InitialVersion,
                                                      date:           DatePublished,
                                                      creators:       NonEmptyList[Person]
    ) extends ImportedInternal {
      override type D = DatePublished
    }

    final case class ImportedInternalAncestorInternal(resourceId:     ResourceId,
                                                      identifier:     Identifier,
                                                      sameAs:         InternalSameAs,
                                                      topmostSameAs:  TopmostSameAs,
                                                      initialVersion: InitialVersion,
                                                      date:           DateCreated,
                                                      creators:       NonEmptyList[Person]
    ) extends ImportedInternal {
      override type D = DateCreated
    }

    final case class Modified(resourceId:            ResourceId,
                              derivedFrom:           DerivedFrom,
                              topmostDerivedFrom:    TopmostDerivedFrom,
                              initialVersion:        InitialVersion,
                              date:                  DateCreated,
                              creators:              NonEmptyList[Person],
                              maybeInvalidationTime: Option[InvalidationTime]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val topmostSameAs: TopmostSameAs = TopmostSameAs(resourceId.asEntityId)
    }

    private[Dataset] sealed trait FixableFailure extends Product with Serializable
    private[Dataset] object FixableFailure {
      case object MissingDerivedFrom extends FixableFailure
    }

    private[Dataset] implicit def encoder(implicit
        renkuBaseUrl: RenkuBaseUrl,
        gitLabApiUrl: GitLabApiUrl
    ): Provenance => Map[Property, JsonLD] = {
      case provenance @ Internal(_, _, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.initialVersion.asJsonLD
        )
      case provenance @ ImportedExternal(_, _, sameAs, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorExternal(_, _, sameAs, topmostSameAs, initialVersion, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorInternal(_, _, sameAs, topmostSameAs, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ Modified(_,
                                 derivedFrom,
                                 topmostDerivedFrom,
                                 initialVersion,
                                 date,
                                 creators,
                                 maybeInvalidationTime
          ) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          prov / "wasDerivedFrom"      -> derivedFrom.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD,
          prov / "invalidatedAtTime"   -> maybeInvalidationTime.asJsonLD
        )
    }

    private[Dataset] def decoder(identification: Identification): JsonLDDecoder[(Provenance, Option[FixableFailure])] =
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
            cursor.downField(prov / "wasDerivedFrom").as[Option[DerivedFrom]](decodeOption(DerivedFrom.jsonLDDecoder))
          maybeOriginalIdentifier <- cursor.downField(renku / "originalIdentifier").as[Option[InitialVersion]]
          maybeInvalidationTime   <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
          provenanceAndFixableFailure <- createProvenance(identification, creators)(maybeDateCreated,
                                                                                    maybeDatePublished,
                                                                                    maybeInternalSameAs,
                                                                                    maybeExternalSameAs,
                                                                                    maybeDerivedFrom,
                                                                                    maybeOriginalIdentifier,
                                                                                    maybeInvalidationTime
                                         )
        } yield provenanceAndFixableFailure
      }

    private def createProvenance(id: Identification, creators: NonEmptyList[Person]): (Option[DateCreated],
                                                                                       Option[DatePublished],
                                                                                       Option[InternalSameAs],
                                                                                       Option[ExternalSameAs],
                                                                                       Option[DerivedFrom],
                                                                                       Option[InitialVersion],
                                                                                       Option[InvalidationTime]
    ) => Result[(Provenance, Option[FixableFailure])] = {
      case (Some(dateCreated), None, None, None, None, maybeOriginalId, None)
          if originalIdEqualCurrentId(maybeOriginalId, id) =>
        (Internal(id.resourceId, id.identifier, dateCreated, creators.sortBy(_.name)) -> None).asRight
      case (Some(dateCreated), None, None, None, None, Some(_), None) =>
        (Internal(id.resourceId,
                  id.identifier,
                  dateCreated,
                  creators.sortBy(_.name)
        ) -> FixableFailure.MissingDerivedFrom.some).asRight
      case (None, Some(datePublished), None, Some(sameAs), None, maybeOriginalId, None)
          if originalIdEqualCurrentId(maybeOriginalId, id) =>
        (ImportedExternal(id.resourceId, id.identifier, sameAs, datePublished, creators.sortBy(_.name)) -> None).asRight
      case (Some(dateCreated), None, Some(sameAs), None, None, maybeInitialVersion, None) =>
        (ImportedInternalAncestorInternal(
          id.resourceId,
          id.identifier,
          sameAs,
          TopmostSameAs(sameAs),
          maybeInitialVersion getOrElse InitialVersion(id.identifier),
          dateCreated,
          creators.sortBy(_.name)
        ) -> None).asRight
      case (None, Some(datePublished), Some(sameAs), None, None, maybeInitialVersion, None) =>
        (ImportedInternalAncestorExternal(
          id.resourceId,
          id.identifier,
          sameAs,
          TopmostSameAs(sameAs),
          maybeInitialVersion getOrElse InitialVersion(id.identifier),
          datePublished,
          creators.sortBy(_.name)
        ) -> None).asRight
      case (Some(dateCreated), None, None, None, Some(derivedFrom), Some(initialVersion), maybeInvalidationTime) =>
        (Modified(id.resourceId,
                  derivedFrom,
                  TopmostDerivedFrom(derivedFrom),
                  initialVersion,
                  dateCreated,
                  creators,
                  maybeInvalidationTime
        ) -> None).asRight
      case (maybeDateCreated,
            maybeDatePublished,
            maybeInternalSameAs,
            maybeExternalSameAs,
            maybeDerivedFrom,
            maybeOriginalIdentifier,
            maybeInvalidationTime
          ) =>
        DecodingFailure(
          "Invalid dataset data " +
            s"dateCreated: $maybeDateCreated, " +
            s"datePublished: $maybeDatePublished, " +
            s"internalSameAs: $maybeInternalSameAs, " +
            s"externalSameAs: $maybeExternalSameAs, " +
            s"derivedFrom: $maybeDerivedFrom, " +
            s"originalIdentifier: $maybeOriginalIdentifier, " +
            s"maybeInvalidationTime: $maybeInvalidationTime",
          Nil
        ).asLeft
    }

    private implicit lazy val creatorsOrdering: Ordering[Person] = Ordering.by(_.name)
  }

  private def originalIdEqualCurrentId(maybeOriginalIdentifier: Option[InitialVersion],
                                       identification:          Identification
  ): Boolean =
    maybeOriginalIdentifier.isEmpty || maybeOriginalIdentifier.exists(_.value == identification.identifier.value)

  final case class AdditionalInfo(
      maybeDescription: Option[Description],
      keywords:         List[Keyword],
      images:           List[Image],
      maybeLicense:     Option[License],
      maybeVersion:     Option[Version]
  )

  object AdditionalInfo {

    private[Dataset] implicit lazy val encoder: AdditionalInfo => Map[Property, JsonLD] = {
      case AdditionalInfo(maybeDescription, keywords, images, maybeLicense, maybeVersion) =>
        Map(
          schema / "description" -> maybeDescription.asJsonLD,
          schema / "keywords"    -> keywords.asJsonLD,
          schema / "image"       -> images.asJsonLD,
          schema / "license"     -> maybeLicense.asJsonLD,
          schema / "version"     -> maybeVersion.asJsonLD
        )
    }

    private[Dataset] implicit lazy val decoder: JsonLDDecoder[AdditionalInfo] = JsonLDDecoder.entity(entityTypes) {
      cursor =>
        import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._
        for {
          maybeDescription <- cursor.downField(schema / "description").as[Option[Description]]
          keywords     <- cursor.downField(schema / "keywords").as[List[Option[Keyword]]].map(_.flatten).map(_.sorted)
          images       <- cursor.downField(schema / "image").as[List[Image]].map(_.sortBy(_.position))
          maybeLicense <- cursor.downField(schema / "license").as[Option[License]]
          maybeVersion <- cursor.downField(schema / "version").as[Option[Version]]
        } yield AdditionalInfo(maybeDescription, keywords, images, maybeLicense, maybeVersion)
    }
  }

  final case class Image(resourceId: ImageResourceId, uri: ImageUri, position: ImagePosition)

  object Image {
    private val imageEntityTypes = EntityTypes of schema / "ImageObject"

    private[Dataset] implicit val jsonLDEncoder: JsonLDEncoder[Image] = JsonLDEncoder.instance {
      case Image(resourceId, uri, position) =>
        JsonLD.entity(
          resourceId.asEntityId,
          imageEntityTypes,
          (schema / "contentUrl") -> uri.asJsonLD,
          (schema / "position")   -> position.asJsonLD
        )
    }

    private[Dataset] implicit lazy val decoder: JsonLDDecoder[Image] = JsonLDDecoder.entity(imageEntityTypes) {
      cursor =>
        for {
          resourceId <- cursor.downEntityId.as[ImageResourceId]
          uri        <- cursor.downField(schema / "contentUrl").as[ImageUri]
          position   <- cursor.downField(schema / "position").as[ImagePosition]
        } yield Image(resourceId, uri, position)
    }
  }

  val entityTypes: EntityTypes = EntityTypes of (schema / "Dataset", prov / "Entity")

  implicit def encoder[P <: Provenance](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[Dataset[P]] = {
    implicit class SerializationOps[T](obj: T) {
      def asJsonLDProperties(implicit encoder: T => Map[Property, JsonLD]): Map[Property, JsonLD] = encoder(obj)
    }

    JsonLDEncoder.instance { dataset =>
      JsonLD
        .entity(
          dataset.resourceId.asEntityId,
          entityTypes,
          List(
            dataset.identification.asJsonLDProperties,
            dataset.provenance.asJsonLDProperties,
            dataset.additionalInfo.asJsonLDProperties
          ).flatten.toMap,
          schema / "hasPart" -> dataset.parts.asJsonLD
        )
    }
  }

  implicit lazy val decoder: JsonLDDecoder[Dataset[Provenance]] = JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
    import Dataset.Provenance.FixableFailure
    import Dataset.Provenance.FixableFailure.MissingDerivedFrom

    def fixProvenanceDate(provenanceAndFixableFailure: (Provenance, Option[FixableFailure]),
                          parts:                       List[DatasetPart]
    ): Provenance = provenanceAndFixableFailure match {
      case (prov: Provenance.Internal, Some(MissingDerivedFrom)) =>
        prov.copy(date = (prov.date :: parts.map(_.dateCreated)).min)
      case (prov, _) => prov
    }

    for {
      identification              <- cursor.as[Identification]
      provenanceAndFixableFailure <- cursor.as(Provenance.decoder(identification))
      additionalInfo              <- cursor.as[AdditionalInfo]
      parts                       <- cursor.downField(schema / "hasPart").as[List[DatasetPart]]
      publicationEvents           <- cursor.focusTop.as(decodeList(PublicationEvent.decoder(identification)))
      dataset <-
        Dataset
          .from(identification,
                fixProvenanceDate(provenanceAndFixableFailure, parts),
                additionalInfo,
                parts,
                publicationEvents
          )
          .toEither
          .leftMap(errors => DecodingFailure(errors.intercalate("; "), Nil))
    } yield dataset
  }
}

trait DatasetOps[+P <: Provenance] {
  self: Dataset[P] =>

  val resourceId: ResourceId = identification.resourceId

  def update(
      topmostSameAs:   TopmostSameAs
  )(implicit evidence: P <:< ImportedInternal, factoryEvidence: TopmostSameAs.type): Dataset[P] =
    provenance match {
      case p: ImportedInternalAncestorInternal =>
        copy(provenance = p.copy(topmostSameAs = topmostSameAs)).asInstanceOf[Dataset[P]]
      case p: ImportedInternalAncestorExternal =>
        copy(provenance = p.copy(topmostSameAs = topmostSameAs)).asInstanceOf[Dataset[P]]
    }

  def update(
      topmostDerivedFrom: TopmostDerivedFrom
  )(implicit evidence:    P <:< Modified, factoryEvidence: TopmostDerivedFrom.type): Dataset[P] =
    provenance match {
      case p: Modified => copy(provenance = p.copy(topmostDerivedFrom = topmostDerivedFrom)).asInstanceOf[Dataset[P]]
    }
}
