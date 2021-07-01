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

package ch.datascience.rdfstore.entities

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.entities.Dataset._
import io.renku.jsonld.JsonLDEncoder.encodeList
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, JsonLD, Property}

import scala.language.implicitConversions

case class Dataset[+P <: Provenance](identification: Identification,
                                     provenance:     P,
                                     additionalInfo: AdditionalInfo,
                                     publishing:     Publishing,
                                     parts:          List[DatasetPart],
                                     project:        Project[Project.ForksCount]
) {

  def entityId(implicit renkuBaseUrl: RenkuBaseUrl): EntityId = Dataset.entityId(identification.identifier)

  validateState(identification.identifier, provenance, project, parts, publishing.publicationEvents)
    .fold(errors => throw new IllegalStateException(errors.nonEmptyIntercalate("; ")), _ => ())
}

object Dataset {

  final case class Identification(
      identifier: Identifier,
      title:      Title,
      name:       Name
  )

  object Identification {
    private[Dataset] implicit lazy val encoder: Identification => Map[Property, JsonLD] = {
      case Identification(identifier, title, name) =>
        Map(
          schema / "identifier"    -> identifier.asJsonLD,
          schema / "name"          -> title.asJsonLD,
          schema / "alternateName" -> name.asJsonLD
        )
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

    final case class Internal(entityId:       EntityId,
                              initialVersion: InitialVersion,
                              date:           DateCreated,
                              creators:       Set[Person]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(entityId)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(entityId)
    }

    final case class ImportedExternal(entityId:       EntityId,
                                      sameAs:         ExternalSameAs,
                                      initialVersion: InitialVersion,
                                      date:           DatePublished,
                                      creators:       Set[Person]
    ) extends Provenance {
      override type D = DatePublished
      override lazy val topmostSameAs:      TopmostSameAs      = TopmostSameAs(sameAs)
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(entityId)
    }

    sealed trait ImportedInternal extends Provenance {
      val entityId:      EntityId
      val sameAs:        InternalSameAs
      val topmostSameAs: TopmostSameAs
      val date:          D
      val creators:      Set[Person]
      override lazy val topmostDerivedFrom: TopmostDerivedFrom = TopmostDerivedFrom(entityId)
    }
    final case class ImportedInternalAncestorExternal(entityId:       EntityId,
                                                      sameAs:         InternalSameAs,
                                                      topmostSameAs:  TopmostSameAs,
                                                      initialVersion: InitialVersion,
                                                      date:           DatePublished,
                                                      creators:       Set[Person]
    ) extends ImportedInternal {
      override type D = DatePublished
    }
    final case class ImportedInternalAncestorInternal(entityId:       EntityId,
                                                      sameAs:         InternalSameAs,
                                                      topmostSameAs:  TopmostSameAs,
                                                      initialVersion: InitialVersion,
                                                      date:           DateCreated,
                                                      creators:       Set[Person]
    ) extends ImportedInternal {
      override type D = DateCreated
    }

    final case class Modified(entityId:           EntityId,
                              derivedFrom:        DerivedFrom,
                              topmostDerivedFrom: TopmostDerivedFrom,
                              initialVersion:     InitialVersion,
                              date:               DateCreated,
                              creators:           Set[Person]
    ) extends Provenance {
      override type D = DateCreated
      override lazy val topmostSameAs: TopmostSameAs = TopmostSameAs(entityId)
    }

    import ch.datascience.graph.model.datasets.DerivedFrom._

    private[Dataset] implicit def encoder(implicit
        renkuBaseUrl: RenkuBaseUrl,
        gitLabApiUrl: GitLabApiUrl
    ): Provenance => Map[Property, JsonLD] = {
      case provenance @ Internal(_, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedExternal(_, sameAs, initialVersion, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> provenance.topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorExternal(_, sameAs, topmostSameAs, initialVersion, date, creators) =>
        Map(
          schema / "datePublished"     -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
        )
      case provenance @ ImportedInternalAncestorInternal(_, sameAs, topmostSameAs, initialVersion, date, creators) =>
        Map(
          schema / "dateCreated"       -> date.asJsonLD,
          schema / "sameAs"            -> sameAs.asJsonLD,
          schema / "creator"           -> creators.asJsonLD,
          renku / "topmostSameAs"      -> topmostSameAs.asJsonLD,
          renku / "topmostDerivedFrom" -> provenance.topmostDerivedFrom.asJsonLD,
          renku / "originalIdentifier" -> initialVersion.asJsonLD
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

    private implicit lazy val creatorsOrdering: Ordering[Person] = Ordering.by((p: Person) => p.name.value)
  }

  final case class Publishing(
      publicationEvents: List[PublicationEvent],
      maybeVersion:      Option[Version]
  )

  object Publishing {
    private[Dataset] implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): Publishing => Map[Property, JsonLD] = {
      case Publishing(publicationEvents, maybeVersion) =>
        Map(
          schema / "subjectOf" -> publicationEvents.asJsonLD,
          schema / "version"   -> maybeVersion.asJsonLD
        )
    }
  }

  final case class AdditionalInfo(
      url:              Url,
      maybeDescription: Option[Description],
      keywords:         List[Keyword],
      images:           List[ImageUri],
      maybeLicense:     Option[License]
  )

  object AdditionalInfo {
    private[Dataset] implicit def encoder(datasetEntityId: EntityId): AdditionalInfo => Map[Property, JsonLD] = {
      case AdditionalInfo(url, maybeDescription, keywords, images, maybeLicense) =>
        Map(
          schema / "url"         -> url.asJsonLD,
          schema / "description" -> maybeDescription.asJsonLD,
          schema / "keywords"    -> keywords.asJsonLD,
          schema / "image"       -> images.zipWithIndex.asJsonLD(encodeList(imageUrlEncoder(datasetEntityId))),
          schema / "license"     -> maybeLicense.asJsonLD
        )
    }

  }

  def from[P <: Provenance](identification: Identification,
                            provenance:     P,
                            additionalInfo: AdditionalInfo,
                            publishing:     Publishing,
                            parts:          List[DatasetPart],
                            project:        Project[Project.ForksCount]
  ): ValidatedNel[String, Dataset[P]] =
    validateState(identification.identifier, provenance, project, parts, publishing.publicationEvents).map(_ =>
      Dataset[P](
        identification,
        provenance,
        additionalInfo,
        publishing,
        parts,
        project
      )
    )

  private[Dataset] def validateState[P <: Provenance](identifier:        Identifier,
                                                      provenance:        P,
                                                      project:           Project[Project.ForksCount],
                                                      parts:             List[DatasetPart],
                                                      publicationEvents: List[PublicationEvent]
  ): ValidatedNel[String, Unit] = List(
    validateDateCreated(identifier, project, provenance),
    validateCreators(identifier, provenance.creators),
    validateParts(identifier, provenance, parts),
    validatePublicationEvents(identifier, provenance, publicationEvents, project)
  ).sequence.void

  private[Dataset] def validateCreators(identifier: Identifier, creators: Set[Person]): ValidatedNel[String, Unit] =
    Validated.condNel(creators.nonEmpty, (), s"No creators on dataset with id: $identifier")

  private[Dataset] def validateDateCreated[P <: Provenance](identifier: Identifier,
                                                            project:    Project[Project.ForksCount],
                                                            provenance: P
  ): ValidatedNel[String, Unit] = provenance match {
    case prov: Provenance.Internal =>
      Validated.condNel(
        test = (prov.date.value compareTo project.topAncestorDateCreated.value) >= 0,
        (),
        s"Internal Dataset with id: $identifier is older than project ${project.name}"
      )
    case prov: Provenance.Modified =>
      Validated.condNel(
        test = (prov.date.value compareTo project.topAncestorDateCreated.value) >= 0,
        (),
        s"Modified Dataset with id: $identifier is older than project ${project.name}"
      )
    case _ => Validated.validNel(())
  }

  private[Dataset] def validateParts[P <: Provenance](
      identifier: Identifier,
      provenance: P,
      parts:      List[DatasetPart]
  ): ValidatedNel[String, Unit] = provenance match {
    case _: Provenance.Modified => Validated.validNel(())
    case prov =>
      parts
        .map { part =>
          Validated.condNel(
            test = (part.dateCreated.value compareTo prov.date.instant) >= 0,
            (),
            s"Part ${part.entity.location} on dataset with id: $identifier is older than dataset date"
          )
        }
        .sequence
        .void
  }

  private[Dataset] def validatePublicationEvents[P <: Provenance](
      identifier:        Identifier,
      provenance:        P,
      publicationEvents: List[PublicationEvent],
      project:           Project[_]
  ): ValidatedNel[String, Unit] = {
    provenance match {
      case prov: Provenance.Internal         => Some(prov.date.instant, "dataset date")
      case _:    Provenance.ImportedExternal => Some(project.topAncestorDateCreated.value, "project date")
      case _ => None
    }
  }.map { case (minDate, dateLabel) =>
    publicationEvents.map { event =>
      Validated.condNel(
        test = (event.startDate.value compareTo minDate) >= 0,
        (),
        s"Publication Event ${event.about} on dataset with id: $identifier is older than $dateLabel"
      )
    }.combineAll
  }.getOrElse(Validated.validNel(()))

  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder[P <: Provenance](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[Dataset[P]] = JsonLDEncoder.instance {
    case dataset: Dataset[P] with HavingInvalidationTime =>
      JsonLD
        .entity(
          dataset.entityId,
          EntityTypes of (schema / "Dataset", prov / "Entity"),
          List(
            dataset.identification.asJsonLDProperties,
            dataset.provenance.asJsonLDProperties,
            dataset.additionalInfo.asJsonLDProperties(AdditionalInfo.encoder(dataset.entityId)),
            dataset.publishing.asJsonLDProperties
          ).flatten.toMap,
          schema / "hasPart"         -> dataset.parts.asJsonLD,
          schema / "isPartOf"        -> dataset.project.asJsonLD,
          prov / "invalidatedAtTime" -> dataset.invalidationTime.asJsonLD
        )
    case dataset @ Dataset(identification, provenance, additionalInfo, publishing, parts, project) =>
      JsonLD
        .entity(
          dataset.entityId,
          EntityTypes of (schema / "Dataset", prov / "Entity"),
          List(
            identification.asJsonLDProperties,
            provenance.asJsonLDProperties,
            additionalInfo.asJsonLDProperties(AdditionalInfo.encoder(dataset.entityId)),
            publishing.asJsonLDProperties
          ).flatten.toMap,
          schema / "hasPart"  -> parts.asJsonLD,
          schema / "isPartOf" -> project.asJsonLD
        )
  }

  private implicit class SerializationOps[T](obj: T) {
    def asJsonLDProperties(implicit encoder: T => Map[Property, JsonLD]): Map[Property, JsonLD] = encoder(obj)
  }

  implicit def entityIdEncoder[P <: Provenance](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Dataset[P]] =
    EntityIdEncoder.instance(dataset => entityId(dataset.identification.identifier))

  def entityId(identifier: DatasetIdentifier)(implicit renkuBaseUrl: RenkuBaseUrl): EntityId =
    EntityId of (renkuBaseUrl / "datasets" / identifier)

  private def imageUrlEncoder(datasetEntityId: EntityId): JsonLDEncoder[(ImageUri, Int)] =
    JsonLDEncoder.instance { case (imageUrl, position) =>
      JsonLD.entity(
        datasetEntityId.asUrlEntityId / "images" / position.toString,
        EntityTypes of schema / "ImageObject",
        (schema / "contentUrl") -> imageUrl.asJsonLD,
        (schema / "position")   -> position.asJsonLD
      )
    }
}
