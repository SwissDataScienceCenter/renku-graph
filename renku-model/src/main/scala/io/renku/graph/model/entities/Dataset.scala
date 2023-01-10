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

package io.renku.graph.model.entities

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model._
import io.renku.graph.model.cli.CliDatasetProvenance
import io.renku.graph.model.datasets._
import io.renku.graph.model.entities.Dataset.Provenance._
import io.renku.graph.model.entities.Dataset._
import io.renku.graph.model.images.Image
import io.renku.jsonld.JsonLDEncoder

import java.time.Instant

final case class Dataset[+P <: Provenance](identification:    Identification,
                                           provenance:        P,
                                           additionalInfo:    AdditionalInfo,
                                           parts:             List[DatasetPart],
                                           publicationEvents: List[PublicationEvent]
) extends DatasetOps[P]

object Dataset {

  implicit def functions[P <: Provenance](implicit
      renkuUrl:     RenkuUrl,
      gitLabApiUrl: GitLabApiUrl
  ): EntityFunctions[Dataset[P]] = new EntityFunctions[Dataset[P]] {

    override val findAllPersons: Dataset[P] => Set[Person] = _.provenance.creators.toList.toSet

    override val encoder: GraphClass => JsonLDEncoder[Dataset[P]] = Dataset.encoder(renkuUrl, gitLabApiUrl, _)
  }

  import io.renku.graph.model.Schemas.{prov, renku, schema}
  import io.renku.jsonld.JsonLDDecoder._
  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld.ontology._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder, Property}

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
    val originalIdentifier: OriginalIdentifier
    val topmostDerivedFrom: TopmostDerivedFrom
    val date:               D
    val creators:           NonEmptyList[Person]
  }

  object Provenance {

    object Internal {
      object FromCli {
        def unapply(cli: CliDatasetProvenance): Option[Internal] =
          cli match {
            case CliDatasetProvenance(id, creators, Some(dateCreated), None, None, None, None, None, _, None) =>
              Some(Internal(id.resourceId, id.identifier, dateCreated, creators.sortBy(_.name)))
            case _ => None
          }
      }
    }

    final case class Internal(resourceId: ResourceId,
                              identifier: Identifier,
                              date:       DateCreated,
                              creators:   NonEmptyList[Person]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val originalIdentifier: OriginalIdentifier = OriginalIdentifier(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(resourceId.asEntityId)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    object ImportedExternal {
      object FromCli {
        def unapply(cliData: CliDatasetProvenance): Option[ImportedExternal] =
          cliData match {
            case CliDatasetProvenance(id, creators, None, Some(datePublished), None, None, Some(sameAs), None, _, None)
                if cliData.originalIdEqualCurrentId =>
              ImportedExternal(id.resourceId, id.identifier, sameAs, datePublished, creators.sortBy(_.name)).some
            case _ =>
              None
          }
      }
    }

    final case class ImportedExternal(resourceId: ResourceId,
                                      identifier: Identifier,
                                      sameAs:     ExternalSameAs,
                                      date:       DatePublished,
                                      creators:   NonEmptyList[Person]
    ) extends Provenance {
      override type D = DatePublished
      override lazy val originalIdentifier: OriginalIdentifier = OriginalIdentifier(identifier)
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(sameAs)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    object ImportedInternal

    sealed trait ImportedInternal extends Provenance {
      val resourceId:         ResourceId
      val identifier:         Identifier
      val sameAs:             InternalSameAs
      val topmostSameAs:      TopmostSameAs
      val date:               D
      val originalIdentifier: OriginalIdentifier
      val creators:           NonEmptyList[Person]

      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(resourceId.asEntityId)
    }

    object ImportedInternalAncestorExternal {
      object FromCli {
        def unapply(cliData: CliDatasetProvenance): Option[ImportedInternalAncestorExternal] =
          cliData match {
            case CliDatasetProvenance(
                  id,
                  creators,
                  None,
                  Some(datePublished),
                  None,
                  Some(sameAs),
                  None,
                  None,
                  maybeOriginalId,
                  None
                ) =>
              ImportedInternalAncestorExternal(
                id.resourceId,
                id.identifier,
                sameAs,
                TopmostSameAs(sameAs),
                maybeOriginalId getOrElse OriginalIdentifier(id.identifier),
                datePublished,
                creators.sortBy(_.name)
              ).some
            case _ => None
          }

      }
    }
    final case class ImportedInternalAncestorExternal(resourceId:         ResourceId,
                                                      identifier:         Identifier,
                                                      sameAs:             InternalSameAs,
                                                      topmostSameAs:      TopmostSameAs,
                                                      originalIdentifier: OriginalIdentifier,
                                                      date:               DatePublished,
                                                      creators:           NonEmptyList[Person]
    ) extends ImportedInternal {
      override type D = DatePublished
    }

    object ImportedInternalAncestorInternal {
      object FromCli {
        def unapply(cliData: CliDatasetProvenance): Option[ImportedInternalAncestorInternal] =
          cliData match {
            case CliDatasetProvenance(
                  id,
                  creators,
                  Some(dateCreated),
                  None,
                  None,
                  Some(sameAs),
                  None,
                  None,
                  maybeOriginalId,
                  None
                ) =>
              ImportedInternalAncestorInternal(
                id.resourceId,
                id.identifier,
                sameAs,
                TopmostSameAs(sameAs),
                maybeOriginalId getOrElse OriginalIdentifier(id.identifier),
                dateCreated,
                creators.sortBy(_.name)
              ).some
            case _ => None
          }
      }
    }
    final case class ImportedInternalAncestorInternal(resourceId:         ResourceId,
                                                      identifier:         Identifier,
                                                      sameAs:             InternalSameAs,
                                                      topmostSameAs:      TopmostSameAs,
                                                      originalIdentifier: OriginalIdentifier,
                                                      date:               DateCreated,
                                                      creators:           NonEmptyList[Person]
    ) extends ImportedInternal {
      override type D = DateCreated
    }

    object Modified {
      object FromCli {
        def unapply(cliData: CliDatasetProvenance): Option[Modified] =
          cliData match {
            case CliDatasetProvenance(
                  id,
                  creators,
                  _,
                  None,
                  Some(dateModified),
                  None,
                  None,
                  Some(derivedFrom),
                  Some(originalId),
                  maybeInvalidationTime
                ) =>
              Modified(id.resourceId,
                       derivedFrom,
                       TopmostDerivedFrom(derivedFrom),
                       originalId,
                       DateCreated(dateModified.value),
                       creators,
                       maybeInvalidationTime
              ).some
            case _ => None
          }
      }
    }
    final case class Modified(resourceId:            ResourceId,
                              derivedFrom:           DerivedFrom,
                              topmostDerivedFrom:    TopmostDerivedFrom,
                              originalIdentifier:    OriginalIdentifier,
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
        renkuUrl: RenkuUrl,
        glApiUrl: GitLabApiUrl,
        graph:    GraphClass
    ): Provenance => Map[Property, JsonLD] = {
      case provenance @ Internal(_, _, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.originalIdentifier.asJsonLD
        )
      case provenance @ ImportedExternal(_, _, sameAs, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> provenance.originalIdentifier.asJsonLD
        )
      case provenance @ ImportedInternalAncestorExternal(_, _, sameAs, topmostSameAs, originalId, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> originalId.asJsonLD
        )
      case provenance @ ImportedInternalAncestorInternal(_, _, sameAs, topmostSameAs, originalId, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.toList.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> originalId.asJsonLD
        )
      case provenance @ Modified(_,
                                 derivedFrom,
                                 topmostDerivedFrom,
                                 originalId,
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
          renku / "originalIdentifier" -> originalId.asJsonLD,
          prov / "invalidatedAtTime"   -> maybeInvalidationTime.asJsonLD
        )
    }
  }

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

  val entityTypes: EntityTypes = EntityTypes of (schema / "Dataset", prov / "Entity")

  implicit def encoder[P <: Provenance](implicit
      renkuUrl:     RenkuUrl,
      gitLabApiUrl: GitLabApiUrl,
      graph:        GraphClass
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

  implicit def decoder(implicit renkuUrl: RenkuUrl): JsonLDDecoder[Dataset[Provenance]] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
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
        cliProvenance               <- cursor.as(CliDatasetProvenance.decoder(identification))
        provenanceAndFixableFailure <- createProvenance(cliProvenance)
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

  private def createProvenance(cliData: CliDatasetProvenance): Result[(Provenance, Option[FixableFailure])] =
    cliData match {
      case Internal.FromCli(p) =>
        // format: off
        if (cliData.originalIdNotEqualCurrentId) (p -> FixableFailure.MissingDerivedFrom.some).asRight
        else (p -> None).asRight
        // format: on

      case ImportedExternal.FromCli(p) => (p -> None).asRight

      case ImportedInternalAncestorInternal.FromCli(p) => (p -> None).asRight

      case ImportedInternalAncestorExternal.FromCli(p) => (p -> None).asRight

      case Modified.FromCli(p) => (p -> None).asRight

      case _ =>
        DecodingFailure(
          "Invalid dataset data " +
            s"identifier: ${cliData.id.identifier}, " +
            s"dateCreated: ${cliData.createdAt}, " +
            s"datePublished: ${cliData.publishedAt}, " +
            s"dateModified: ${cliData.modifiedAt}, " +
            s"internalSameAs: ${cliData.internalSameAs}, " +
            s"externalSameAs: ${cliData.externalSameAs}, " +
            s"derivedFrom: ${cliData.derivedFrom}, " +
            s"originalIdentifier: ${cliData.originalIdentifier}, " +
            s"maybeInvalidationTime: ${cliData.invalidationTime}",
          Nil
        ).asLeft
    }

  val ontologyClass: Class = Class(schema / "Dataset", ParentClass(prov / "Entity"))
  lazy val ontology: Type =
    Type.Def(
      ontologyClass,
      ObjectProperties(
        ObjectProperty(schema / "sameAs", SameAs.ontology),
        ObjectProperty(prov / "wasDerivedFrom", DerivedFrom.ontology),
        ObjectProperty(schema / "creator", Person.ontology),
        ObjectProperty(renku / "topmostSameAs", ontologyClass),
        ObjectProperty(renku / "topmostDerivedFrom", ontologyClass),
        ObjectProperty(schema / "image", Image.ontology),
        ObjectProperty(schema / "hasPart", DatasetPart.ontology)
      ),
      DataProperties(
        DataProperty(schema / "identifier", xsd / "string"),
        DataProperty(schema / "name", xsd / "string"),
        DataProperty(renku / "slug", xsd / "string"),
        DataProperty(schema / "dateCreated", xsd / "dateTime"),
        DataProperty(schema / "datePublished", xsd / "date"),
        DataProperty(renku / "originalIdentifier", xsd / "string"),
        DataProperty(prov / "invalidatedAtTime", xsd / "dateTime"),
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(schema / "keywords", xsd / "string"),
        DataProperty(schema / "license", xsd / "string"),
        DataProperty(schema / "version", xsd / "string")
      ),
      ReverseProperties(PublicationEvent.ontology)
    )
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
