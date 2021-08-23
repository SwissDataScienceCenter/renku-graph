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

package ch.datascience.graph.model.testentities

import Dataset._
import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import ch.datascience.graph.model._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities.Dataset.Provenance.{ImportedExternal, ImportedInternal, ImportedInternalAncestorExternal, ImportedInternalAncestorInternal, Internal, Modified}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

case class Dataset[+P <: Provenance](identification:            Identification,
                                     provenance:                P,
                                     additionalInfo:            AdditionalInfo,
                                     parts:                     List[DatasetPart],
                                     publicationEventFactories: List[Dataset[Provenance] => PublicationEvent],
                                     project:                   Project[ForksCount]
) {

  val publicationEvents: List[PublicationEvent] = publicationEventFactories.map(_.apply(this))

  def entityId(implicit renkuBaseUrl: RenkuBaseUrl): EntityId = Dataset.entityId(identification.identifier)

  validateState(identification.identifier, provenance, parts, publicationEvents, project)
    .fold(errors => throw new IllegalStateException(errors.nonEmptyIntercalate("; ")), _ => ())
}

object Dataset {

  final case class Identification(
      identifier: Identifier,
      title:      Title,
      name:       Name
  )

  sealed trait Provenance extends Product with Serializable {
    type D <: Date
    val topmostSameAs:      TopmostSameAs
    val initialVersion:     InitialVersion
    val topmostDerivedFrom: TopmostDerivedFrom
    val date:               D
    val creators:           Set[Person]
  }

  object Provenance extends ProvenanceOps {

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

    sealed trait ImportedInternal extends Provenance with Product with Serializable {
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
  }

  trait ProvenanceOps {

    implicit def toEntitiesProvenance(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => Provenance => entities.Dataset.Provenance =
      identification => {
        case p: Internal => toEntitiesInternal(renkuBaseUrl)(identification)(p)
        case p: ImportedExternal => toEntitiesImportedExternal(renkuBaseUrl)(identification)(p)
        case p: ImportedInternalAncestorExternal =>
          toEntitiesImportedInternalAncestorExternal(renkuBaseUrl)(identification)(p)
        case p: ImportedInternalAncestorInternal =>
          toEntitiesImportedInternalAncestorInternal(renkuBaseUrl)(identification)(p)
        case p: Modified => toEntitiesModified(renkuBaseUrl)(identification)(p)
      }

    implicit def toEntitiesImportedInternal(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => ImportedInternal => entities.Dataset.Provenance.ImportedInternal =
      identification => {
        case p: ImportedInternalAncestorExternal =>
          toEntitiesImportedInternalAncestorExternal(renkuBaseUrl)(identification)(p)
        case p: ImportedInternalAncestorInternal =>
          toEntitiesImportedInternalAncestorInternal(renkuBaseUrl)(identification)(p)
      }

    implicit def toEntitiesInternal(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => Provenance.Internal => entities.Dataset.Provenance.Internal =
      identification => { case Internal(_, _, date, creators) =>
        entities.Dataset.Provenance.Internal(identification.resourceId,
                                             identification.identifier,
                                             date,
                                             creators.map(_.to[entities.Person])
        )
      }

    implicit def toEntitiesImportedExternal(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => Provenance.ImportedExternal => entities.Dataset.Provenance.ImportedExternal =
      identification => { case ImportedExternal(_, sameAs, _, date, creators) =>
        entities.Dataset.Provenance.ImportedExternal(identification.resourceId,
                                                     identification.identifier,
                                                     sameAs,
                                                     date,
                                                     creators.map(_.to[entities.Person])
        )
      }

    implicit def toEntitiesImportedInternalAncestorExternal(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => ImportedInternalAncestorExternal => entities.Dataset.Provenance.ImportedInternalAncestorExternal =
      identification => { case ImportedInternalAncestorExternal(_, sameAs, topmostSameAs, _, date, creators) =>
        entities.Dataset.Provenance.ImportedInternalAncestorExternal(identification.resourceId,
                                                                     identification.identifier,
                                                                     sameAs,
                                                                     topmostSameAs,
                                                                     date,
                                                                     creators.map(_.to[entities.Person])
        )
      }

    implicit def toEntitiesImportedInternalAncestorInternal(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => ImportedInternalAncestorInternal => entities.Dataset.Provenance.ImportedInternalAncestorInternal =
      identification => { case ImportedInternalAncestorInternal(_, sameAs, topmostSameAs, _, date, creators) =>
        entities.Dataset.Provenance.ImportedInternalAncestorInternal(identification.resourceId,
                                                                     identification.identifier,
                                                                     sameAs,
                                                                     topmostSameAs,
                                                                     date,
                                                                     creators.map(_.to[entities.Person])
        )
      }

    implicit def toEntitiesModified(implicit
        renkuBaseUrl: RenkuBaseUrl
    ): entities.Dataset.Identification => Provenance.Modified => entities.Dataset.Provenance.Modified =
      identification => { case Modified(_, derivedFrom, topmostDerivedFrom, initialVersion, date, creators) =>
        entities.Dataset.Provenance.Modified(identification.resourceId,
                                             derivedFrom,
                                             topmostDerivedFrom,
                                             initialVersion,
                                             date,
                                             creators.map(_.to[entities.Person])
        )
      }
  }

  final case class AdditionalInfo(
      url:              Url,
      maybeDescription: Option[Description],
      keywords:         List[Keyword],
      images:           List[ImageUri],
      maybeLicense:     Option[License],
      maybeVersion:     Option[Version]
  )

  def from[P <: Provenance](identification:            Identification,
                            provenance:                P,
                            additionalInfo:            AdditionalInfo,
                            parts:                     List[DatasetPart],
                            publicationEventFactories: List[Dataset[Provenance] => PublicationEvent],
                            project:                   Project[ForksCount]
  ): ValidatedNel[String, Dataset[P]] =
    validateState(identification.identifier, provenance, parts, publicationEvents = Nil, project)
      .map(_ =>
        Dataset[P](
          identification,
          provenance,
          additionalInfo,
          parts,
          publicationEventFactories = Nil,
          project
        )
      )
      .andThen { dataset =>
        validatePublicationEvents(identification.identifier,
                                  provenance,
                                  publicationEventFactories.map(_.apply(dataset)),
                                  project
        ).map(_ => dataset.copy(publicationEventFactories = publicationEventFactories))
      }

  private[Dataset] def validateState[P <: Provenance](identifier:        Identifier,
                                                      provenance:        P,
                                                      parts:             List[DatasetPart],
                                                      publicationEvents: List[PublicationEvent],
                                                      project:           Project[ForksCount]
  ): ValidatedNel[String, Unit] = List(
    validateDateCreated(identifier, project, provenance),
    validateCreators(identifier, provenance.creators),
    validateParts(identifier, provenance, parts),
    validatePublicationEvents(identifier, provenance, publicationEvents, project)
  ).sequence.void

  private[Dataset] def validateCreators(identifier: Identifier, creators: Set[Person]): ValidatedNel[String, Unit] =
    Validated.condNel(creators.nonEmpty, (), s"No creators on dataset with id: $identifier")

  private[Dataset] def validateDateCreated[P <: Provenance](identifier: Identifier,
                                                            project:    Project[ForksCount],
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

  implicit def toEntitiesDataset[TP <: Provenance, EP <: entities.Dataset.Provenance](implicit
      convert:      entities.Dataset.Identification => TP => EP,
      renkuBaseUrl: RenkuBaseUrl
  ): Dataset[TP] => entities.Dataset[EP] = { dataset: Dataset[TP] =>
    val maybeInvalidationTime = dataset match {
      case d: Dataset[TP] with HavingInvalidationTime => d.invalidationTime.some
      case _ => None
    }
    val identification = entities.Dataset.Identification(ResourceId((dataset: Dataset[Provenance]).asEntityId.show),
                                                         dataset.identification.identifier,
                                                         dataset.identification.title,
                                                         dataset.identification.name
    )
    entities.Dataset(
      identification,
      dataset.provenance.to(convert(identification)),
      entities.Dataset.AdditionalInfo(
        dataset.additionalInfo.url,
        dataset.additionalInfo.maybeDescription,
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images.zipWithIndex.map { case (url, idx) =>
          val imagePosition = ImagePosition(idx)
          entities.Dataset.Image(
            ImageResourceId(imageEntityId((dataset: Dataset[Provenance]).asEntityId, imagePosition).show),
            url,
            imagePosition
          )
        },
        dataset.additionalInfo.maybeLicense,
        dataset.additionalInfo.maybeVersion
      ),
      dataset.parts.map(_.to[entities.DatasetPart]),
      dataset.publicationEvents.map(_.to[entities.PublicationEvent]),
      projects.ResourceId(dataset.project.asEntityId.show),
      maybeInvalidationTime
    )
  }

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder[P <: Provenance](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[Dataset[P]] = JsonLDEncoder.instance { ds =>
    JsonLD.arr(
      ds.project.asJsonLD,
      ds.to[entities.Dataset[entities.Dataset.Provenance]].asJsonLD
    )
  }

  implicit def entityIdEncoder[P <: Provenance](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Dataset[P]] =
    EntityIdEncoder.instance(dataset => entityId(dataset.identification.identifier))

  def entityId(identifier: DatasetIdentifier)(implicit renkuBaseUrl: RenkuBaseUrl): EntityId =
    EntityId of (renkuBaseUrl / "datasets" / identifier)

  private def imageEntityId(datasetEntityId: EntityId, position: ImagePosition): UrlfiedEntityId =
    datasetEntityId.asUrlEntityId / "images" / position.toString
}
