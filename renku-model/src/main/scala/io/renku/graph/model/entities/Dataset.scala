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
import io.renku.cli.model.{CliDataset, CliDatasetProvenance}
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

    override val encoder: GraphClass => JsonLDEncoder[Dataset[P]] = { gc =>
      Dataset.encoder(renkuUrl, gitLabApiUrl, gc, Dataset.Provenance.encoder(renkuUrl, gitLabApiUrl, gc))
    }
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

    def fromCli(dataset: CliDataset): Identification =
      Identification(dataset.resourceId, dataset.identifier, dataset.title, dataset.name)
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

    implicit object Internal {
      object FromCli {
        def unapply(cli: CliDataset): Option[NonEmptyList[Person] => Internal] =
          cli.provenance match {
            case CliDatasetProvenance(dateCreated: DateCreated, None, None, None, _, None) =>
              Some(creators => Internal(cli.resourceId, cli.identifier, dateCreated, creators.sortBy(_.name)))
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

    implicit object ImportedExternal {
      object FromCli {
        def unapply(cliData: CliDataset): Option[NonEmptyList[Person] => ImportedExternal] =
          cliData.provenance match {
            case CliDatasetProvenance(datePublished: DatePublished, None, Some(sameAs), None, _, None)
                if cliData.originalIdEqualCurrentId =>
              Some(creators =>
                ImportedExternal(cliData.resourceId,
                                 cliData.identifier,
                                 sameAs.toExternalSameAs,
                                 datePublished,
                                 creators.sortBy(_.name)
                )
              )
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

    implicit object ImportedInternal
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
        def unapply(cliData: CliDataset): Option[NonEmptyList[Person] => ImportedInternalAncestorExternal] =
          cliData.provenance match {
            case CliDatasetProvenance(datePublished: DatePublished, None, Some(sameAs), None, maybeOriginalId, None) =>
              Some(creators =>
                ImportedInternalAncestorExternal(
                  cliData.resourceId,
                  cliData.identifier,
                  sameAs.toInternalSameAs,
                  TopmostSameAs(sameAs.toInternalSameAs),
                  maybeOriginalId getOrElse OriginalIdentifier(cliData.identifier),
                  datePublished,
                  creators.sortBy(_.name)
                )
              )
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
        def unapply(cliData: CliDataset): Option[NonEmptyList[Person] => ImportedInternalAncestorInternal] =
          cliData.provenance match {
            case CliDatasetProvenance(dateCreated: DateCreated, None, Some(sameAs), None, maybeOriginalId, None) =>
              Some(creators =>
                ImportedInternalAncestorInternal(
                  cliData.resourceId,
                  cliData.identifier,
                  sameAs.toInternalSameAs,
                  TopmostSameAs(sameAs.toInternalSameAs),
                  maybeOriginalId getOrElse OriginalIdentifier(cliData.identifier),
                  dateCreated,
                  creators.sortBy(_.name)
                )
              )
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
        def unapply(cliData: CliDataset): Option[NonEmptyList[Person] => Modified] =
          cliData.provenance match {
            case CliDatasetProvenance(_,
                                      Some(dateModified),
                                      None,
                                      Some(derivedFrom),
                                      Some(originalId),
                                      maybeInvalidationTime
                ) =>
              Some(creators =>
                Modified(cliData.resourceId,
                         derivedFrom,
                         TopmostDerivedFrom(derivedFrom),
                         originalId,
                         DateCreated(dateModified.value),
                         creators,
                         maybeInvalidationTime
                )
              )
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

    implicit def encoder(implicit
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

    def fromCli(dataset: CliDataset): AdditionalInfo =
      AdditionalInfo(
        dataset.description,
        dataset.keywords,
        dataset.images,
        dataset.license,
        dataset.version
      )
  }

  val entityTypes: EntityTypes = EntityTypes of (schema / "Dataset", prov / "Entity")

  implicit def encoder[P <: Provenance](implicit
      renkuUrl:          RenkuUrl,
      gitLabApiUrl:      GitLabApiUrl,
      graph:             GraphClass,
      provenanceEncoder: Provenance => Map[Property, JsonLD]
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
        cliDataset <- cursor.as[CliDataset]
        identification = Identification.fromCli(cliDataset)
        additionalInfo = AdditionalInfo.fromCli(cliDataset)
        parts <- cliDataset.datasetFiles
                   .traverse(DatasetPart.fromCli)
                   .toEither
                   .leftMap(errs => DecodingFailure(errs.intercalate("; "), Nil))
        provenanceAndFixableFailure <- createProvenance(cliDataset)
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

  private def createProvenance(cliData: CliDataset): Result[(Provenance, Option[FixableFailure])] = {
    val creators: Result[NonEmptyList[Person]] =
      cliData.creators.traverse(Person.fromCli).toEither.leftMap(errs => DecodingFailure(errs.intercalate("; "), Nil))

    cliData match {
      case Internal.FromCli(p) =>
        if (cliData.originalIdNotEqualCurrentId) creators.map(p).map(_ -> FixableFailure.MissingDerivedFrom.some)
        else creators.map(p).map(_ -> None)

      case ImportedExternal.FromCli(p) => creators.map(p).map(_ -> None)

      case ImportedInternalAncestorInternal.FromCli(p) => creators.map(p).map(_ -> None)

      case ImportedInternalAncestorExternal.FromCli(p) => creators.map(p).map(_ -> None)

      case Modified.FromCli(p) => creators.map(p).map(_ -> None)

      case _ =>
        DecodingFailure(
          "Invalid dataset data: " +
            s"identifier: ${cliData.identifier}, " +
            show"createdOrPublished: ${cliData.createdOrPublished}, " +
            s"dateModified: ${cliData.dateModified}, " +
            s"sameAs: ${cliData.sameAs}, " +
            s"derivedFrom: ${cliData.derivedFrom}, " +
            s"originalIdentifier: ${cliData.originalIdentifier}, " +
            s"maybeInvalidationTime: ${cliData.invalidationTime}",
          Nil
        ).asLeft
    }
  }

  object Ontology {

    val ontologyClass: Class = Class(schema / "Dataset", ParentClass(prov / "Entity"))

    private val urlToDatasetReference =
      Type.Def(
        Class(schema / "URL"),
        ObjectProperty(schema / "url", ontologyClass)
      )

    val sameAs:             Property = schema / "sameAs"
    val wasDerivedFrom:     Property = prov / "wasDerivedFrom"
    val creator:            Property = schema / "creator"
    val topmostSameAs:      Property = renku / "topmostSameAs"
    val topmostDerivedFrom: Property = renku / "topmostDerivedFrom"
    val image:              Property = schema / "image"
    val hasPart:            Property = schema / "hasPart"

    val identifierProperty:         DataProperty.Def = DataProperty(schema / "identifier", xsd / "string")
    val nameProperty:               DataProperty.Def = DataProperty(schema / "name", xsd / "string")
    val slugProperty:               DataProperty.Def = DataProperty(renku / "slug", xsd / "string")
    val dateCreatedProperty:        DataProperty.Def = DataProperty(schema / "dateCreated", xsd / "dateTime")
    val datePublishedProperty:      DataProperty.Def = DataProperty(schema / "datePublished", xsd / "date")
    val originalIdentifierProperty: DataProperty.Def = DataProperty(renku / "originalIdentifier", xsd / "string")
    val invalidatedAtTimeProperty:  DataProperty.Def = DataProperty(prov / "invalidatedAtTime", xsd / "dateTime")
    val descriptionProperty:        DataProperty.Def = DataProperty(schema / "description", xsd / "string")
    val keywordsProperty:           DataProperty.Def = DataProperty(schema / "keywords", xsd / "string")
    val licenseProperty:            DataProperty.Def = DataProperty(schema / "license", xsd / "string")
    val versionProperty:            DataProperty.Def = DataProperty(schema / "version", xsd / "string")

    lazy val typeDef: Type =
      Type.Def(
        ontologyClass,
        ObjectProperties(
          ObjectProperty(sameAs, urlToDatasetReference),
          ObjectProperty(wasDerivedFrom, urlToDatasetReference),
          ObjectProperty(creator, Person.Ontology.typeDef),
          ObjectProperty(topmostSameAs, ontologyClass),
          ObjectProperty(topmostDerivedFrom, ontologyClass),
          ObjectProperty(image, Image.Ontology.typeDef),
          ObjectProperty(hasPart, DatasetPart.ontology)
        ),
        DataProperties(
          identifierProperty,
          nameProperty,
          slugProperty,
          dateCreatedProperty,
          datePublishedProperty,
          originalIdentifierProperty,
          invalidatedAtTimeProperty,
          descriptionProperty,
          keywordsProperty,
          licenseProperty,
          versionProperty
        ),
        ReverseProperties(PublicationEvent.ontology)
      )
  }
}

trait DatasetOps[+P <: Provenance] {
  self: Dataset[P] =>

  val resourceId: ResourceId = identification.resourceId

  def update(
      topmostSameAs: TopmostSameAs
  )(implicit evidence: P <:< ImportedInternal, factoryEvidence: TopmostSameAs.type): Dataset[P] =
    provenance match {
      case p: ImportedInternalAncestorInternal =>
        copy(provenance = p.copy(topmostSameAs = topmostSameAs)).asInstanceOf[Dataset[P]]
      case p: ImportedInternalAncestorExternal =>
        copy(provenance = p.copy(topmostSameAs = topmostSameAs)).asInstanceOf[Dataset[P]]
    }

  def update(
      topmostDerivedFrom: TopmostDerivedFrom
  )(implicit evidence: P <:< Modified, factoryEvidence: TopmostDerivedFrom.type): Dataset[P] =
    provenance match {
      case p: Modified => copy(provenance = p.copy(topmostDerivedFrom = topmostDerivedFrom)).asInstanceOf[Dataset[P]]
    }
}
