/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.entities.Dataset.Provenance.{ImportedInternal, ImportedInternalAncestorExternal, ImportedInternalAncestorInternal, Modified}
import ch.datascience.graph.model.entities.Dataset.{Provenance, _}
import ch.datascience.graph.model.{GitLabApiUrl, InvalidationTime, projects}
import io.circe.DecodingFailure

import java.time.Instant

final case class Dataset[+P <: Provenance](identification:        Identification,
                                           provenance:            P,
                                           additionalInfo:        AdditionalInfo,
                                           publishing:            Publishing,
                                           parts:                 List[DatasetPart],
                                           projectResourceId:     projects.ResourceId,
                                           maybeInvalidationTime: Option[InvalidationTime]
) extends DatasetOps[P]

object Dataset {

  import ch.datascience.graph.model.Schemas._
  import io.renku.jsonld.JsonLDDecoder._
  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def from[P <: Provenance](identification:        Identification,
                            provenance:            P,
                            additionalInfo:        AdditionalInfo,
                            publishing:            Publishing,
                            parts:                 List[DatasetPart],
                            projectResourceId:     projects.ResourceId,
                            maybeInvalidationTime: Option[InvalidationTime]
  ): ValidatedNel[String, Dataset[P]] = List(
    validateDates(parts, identification)(provenance),
    validate(maybeInvalidationTime, identification)(provenance)
  ).sequence.map { _ =>
    Dataset(identification, provenance, additionalInfo, publishing, parts, projectResourceId, maybeInvalidationTime)
  }

  private def validateDates[P <: Provenance](parts:          List[DatasetPart],
                                             identification: Identification
  ): P => ValidatedNel[String, Unit] = {
    case p: Provenance.Internal                         => verifyDate(parts, identification, p.date.value)
    case p: Provenance.ImportedExternal                 => verifyDate(parts, identification, p.date.instant)
    case p: Provenance.ImportedInternalAncestorExternal => verifyDate(parts, identification, p.date.instant)
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
          schema / "identifier"    -> identifier.asJsonLD,
          schema / "name"          -> title.asJsonLD,
          schema / "alternateName" -> name.asJsonLD
        )
    }

    private[Dataset] implicit lazy val decoder: JsonLDDecoder[Identification] = JsonLDDecoder.entity(entityTypes) {
      cursor =>
        for {
          resourceId <- cursor.downEntityId.as[ResourceId]
          identifier <- cursor.downField(schema / "identifier").as[Identifier]
          title      <- cursor.downField(schema / "name").as[Title]
          name       <- cursor.downField(schema / "alternateName").as[Name]
        } yield Identification(resourceId, identifier, title, name)
    }
  }

  sealed trait Provenance extends Product with Serializable {
    type D <: Date
    val topmostSameAs:      TopmostSameAs
    val initialVersion:     InitialVersion
    val topmostDerivedFrom: TopmostDerivedFrom
    val date:               D
    val creators:           Set[Person]
  }

  object Provenance {

    implicit object Internal
    final case class Internal(resourceId: ResourceId, identifier: Identifier, date: DateCreated, creators: Set[Person])
        extends Provenance {
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
                                      creators:   Set[Person]
    ) extends Provenance {
      override type D = DatePublished
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(sameAs)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    implicit object ImportedInternal
    sealed trait ImportedInternal extends Provenance {
      val resourceId:    ResourceId
      val identifier:    Identifier
      val sameAs:        InternalSameAs
      val topmostSameAs: TopmostSameAs
      val date:          D
      val creators:      Set[Person]

      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
      override lazy val initialVersion:     InitialVersion     = InitialVersion(identifier)
    }

    final case class ImportedInternalAncestorExternal(resourceId:    ResourceId,
                                                      identifier:    Identifier,
                                                      sameAs:        InternalSameAs,
                                                      topmostSameAs: TopmostSameAs,
                                                      date:          DatePublished,
                                                      creators:      Set[Person]
    ) extends ImportedInternal {
      override type D = DatePublished
    }

    final case class ImportedInternalAncestorInternal(resourceId:    ResourceId,
                                                      identifier:    Identifier,
                                                      sameAs:        InternalSameAs,
                                                      topmostSameAs: TopmostSameAs,
                                                      date:          DateCreated,
                                                      creators:      Set[Person]
    ) extends ImportedInternal {
      override type D = DateCreated
    }

    final case class Modified(resourceId:         ResourceId,
                              derivedFrom:        DerivedFrom,
                              topmostDerivedFrom: TopmostDerivedFrom,
                              initialVersion:     InitialVersion,
                              date:               DateCreated,
                              creators:           Set[Person]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val topmostSameAs: TopmostSameAs = TopmostSameAs(resourceId.asEntityId)
    }

    private[Dataset] implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl): Provenance => Map[Property, JsonLD] = {
      case provenance @ Internal(_, _, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.initialVersion.asJsonLD
        )
      case provenance @ ImportedExternal(_, _, sameAs, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorExternal(_, _, sameAs, topmostSameAs, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorInternal(_, _, sameAs, topmostSameAs, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.initialVersion.asJsonLD
        )
      case provenance @ Modified(_, derivedFrom, topmostDerivedFrom, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          prov / "wasDerivedFrom"      -> derivedFrom.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
    }

    private[Dataset] def decoder(identification: Identification): JsonLDDecoder[Provenance] =
      JsonLDDecoder.entity(entityTypes) { cursor =>
        for {
          creators           <- cursor.downField(schema / "creator").as[List[Person]]
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
          maybeInitialVersion <- cursor.downField(renku / "originalIdentifier").as[Option[InitialVersion]]
          provenance <- createProvenance(identification, creators.toSet)(maybeDateCreated,
                                                                         maybeDatePublished,
                                                                         maybeInternalSameAs,
                                                                         maybeExternalSameAs,
                                                                         maybeDerivedFrom,
                                                                         maybeInitialVersion
                        )
        } yield provenance
      }

    private def createProvenance(identification: Identification, creators: Set[Person]): (Option[DateCreated],
                                                                                          Option[DatePublished],
                                                                                          Option[InternalSameAs],
                                                                                          Option[ExternalSameAs],
                                                                                          Option[DerivedFrom],
                                                                                          Option[InitialVersion]
    ) => Result[Provenance] = {
      case (Some(dateCreated), None, None, None, None, _) =>
        Internal(identification.resourceId, identification.identifier, dateCreated, creators).asRight
      case (None, Some(datePublished), None, Some(sameAs), None, _) =>
        ImportedExternal(identification.resourceId, identification.identifier, sameAs, datePublished, creators).asRight
      case (Some(dateCreated), None, Some(sameAs), None, None, _) =>
        ImportedInternalAncestorInternal(identification.resourceId,
                                         identification.identifier,
                                         sameAs,
                                         TopmostSameAs(sameAs),
                                         dateCreated,
                                         creators
        ).asRight
      case (None, Some(datePublished), Some(sameAs), None, None, _) =>
        ImportedInternalAncestorExternal(identification.resourceId,
                                         identification.identifier,
                                         sameAs,
                                         TopmostSameAs(sameAs),
                                         datePublished,
                                         creators
        ).asRight
      case (Some(dateCreated), None, None, None, Some(derivedFrom), Some(initialVersion)) =>
        Modified(identification.resourceId,
                 derivedFrom,
                 TopmostDerivedFrom(derivedFrom),
                 initialVersion,
                 dateCreated,
                 creators
        ).asRight
      case (maybeDateCreated,
            maybeDatePublished,
            maybeInternalSameAs,
            maybeExternalSameAs,
            maybeDerivedFrom,
            maybeInitialVersion
          ) =>
        DecodingFailure(
          "Invalid dataset data " +
            s"dateCreated: $maybeDateCreated, " +
            s"datePublished: $maybeDatePublished, " +
            s"internalSameAs: $maybeInternalSameAs, " +
            s"externalSameAs: $maybeExternalSameAs, " +
            s"derivedFrom: $maybeDerivedFrom, " +
            s"initialVersion: $maybeInitialVersion",
          Nil
        ).asLeft
    }

    private implicit lazy val creatorsOrdering: Ordering[Person] = Ordering.by(_.name.value)
  }

  final case class Publishing(
      publicationEvents: List[PublicationEvent],
      maybeVersion:      Option[Version]
  )

  object Publishing {
    private[Dataset] implicit lazy val encoder: Publishing => Map[Property, JsonLD] = {
      case Publishing(publicationEvents, maybeVersion) =>
        Map(
          schema / "subjectOf" -> publicationEvents.asJsonLD,
          schema / "version"   -> maybeVersion.asJsonLD
        )
    }
    private[Dataset] implicit lazy val decoder: JsonLDDecoder[Publishing] = JsonLDDecoder.entity(entityTypes) {
      cursor =>
        for {
          publicationEvents <- cursor.downField(schema / "subjectOf").as[List[PublicationEvent]]
          maybeVersion      <- cursor.downField(schema / "version").as[Option[Version]]
        } yield Publishing(publicationEvents, maybeVersion)
    }
  }

  final case class AdditionalInfo(
      url:              Url,
      maybeDescription: Option[Description],
      keywords:         List[Keyword],
      images:           List[Image],
      maybeLicense:     Option[License]
  )

  object AdditionalInfo {

    private[Dataset] implicit lazy val encoder: AdditionalInfo => Map[Property, JsonLD] = {
      case AdditionalInfo(url, maybeDescription, keywords, images, maybeLicense) =>
        Map(
          schema / "url"         -> url.asJsonLD,
          schema / "description" -> maybeDescription.asJsonLD,
          schema / "keywords"    -> keywords.asJsonLD,
          schema / "image"       -> images.asJsonLD,
          schema / "license"     -> maybeLicense.asJsonLD
        )
    }

    private[Dataset] implicit lazy val decoder: JsonLDDecoder[AdditionalInfo] = JsonLDDecoder.entity(entityTypes) {
      cursor =>
        for {
          url              <- cursor.downField(schema / "url").as[Url]
          maybeDescription <- cursor.downField(schema / "description").as[Option[Description]]
          keywords         <- cursor.downField(schema / "keywords").as[List[Keyword]].map(_.sorted)
          images           <- cursor.downField(schema / "image").as[List[Image]].map(_.sortBy(_.position))
          maybeLicense     <- cursor.downField(schema / "license").as[Option[License]]
        } yield AdditionalInfo(url, maybeDescription, keywords, images, maybeLicense)
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

  implicit def encoder[P <: Provenance](implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Dataset[P]] = {
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
            dataset.additionalInfo.asJsonLDProperties,
            dataset.publishing.asJsonLDProperties
          ).flatten.toMap,
          schema / "hasPart"         -> dataset.parts.asJsonLD,
          schema / "isPartOf"        -> dataset.projectResourceId.asEntityId.asJsonLD,
          prov / "invalidatedAtTime" -> dataset.maybeInvalidationTime.asJsonLD
        )
    }
  }

  implicit lazy val decoder: JsonLDDecoder[Dataset[Provenance]] = JsonLDDecoder.entity(entityTypes) { cursor =>
    for {
      identification        <- cursor.as[Identification]
      provenance            <- cursor.as[Provenance](Provenance.decoder(identification))
      additionalInfo        <- cursor.as[AdditionalInfo]
      publishing            <- cursor.as[Publishing]
      parts                 <- cursor.downField(schema / "hasPart").as[List[DatasetPart]]
      projectResourceId     <- cursor.downField(schema / "isPartOf").downEntityId.as[projects.ResourceId]
      maybeInvalidationTime <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
      dataset <-
        Dataset
          .from(identification, provenance, additionalInfo, publishing, parts, projectResourceId, maybeInvalidationTime)
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
