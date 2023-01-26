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

package io.renku.graph.model.testentities
package generators

import cats.data.{Kleisli, NonEmptyList}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{fixed, nonBlankStrings, positiveInts, sentences, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.datasets.{DerivedFrom, ExternalSameAs, Identifier, InternalSameAs, OriginalIdentifier, TopmostSameAs}
import io.renku.graph.model.testentities.Dataset.{AdditionalInfo, Identification, Provenance}
import io.renku.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory
import io.renku.tinytypes.InstantTinyType
import monocle.Lens
import org.scalacheck.Gen
import org.scalacheck.Gen.oneOf

import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

trait DatasetEntitiesGenerators {
  self: EntitiesGenerators =>

  type ProvenanceGen[+P <: Provenance] = (Identifier, InstantTinyType) => RenkuUrl => Gen[P]

  def datasetEntities[P <: Dataset.Provenance](
      provenanceGen:     ProvenanceGen[P],
      identificationGen: Gen[Identification] = datasetIdentifications,
      additionalInfoGen: Gen[AdditionalInfo] = datasetAdditionalInfos
  )(implicit renkuUrl: RenkuUrl): DatasetGenFactory[P] = projectDateCreated =>
    for {
      identification <- identificationGen
      provenance     <- provenanceGen(identification.identifier, projectDateCreated)(renkuUrl)
      additionalInfo <- additionalInfoGen
      parts          <- datasetPartEntities(provenance.date.instant).toGeneratorOfList()
      publicationEventFactories <- publicationEventFactories {
                                     provenance.date match {
                                       case dateCreated: datasets.DateCreated => dateCreated.value
                                       case _ => projectDateCreated.value
                                     }
                                   }.toGeneratorOfList()
    } yield Dataset
      .from(
        identification,
        provenance,
        additionalInfo,
        parts,
        publicationEventFactories,
        projectDateCreated
      )
      .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  def datasetAndModificationEntities[P <: Dataset.Provenance](
      provenance:             ProvenanceGen[P],
      projectDateCreated:     projects.DateCreated = projects.DateCreated(Instant.EPOCH),
      modificationCreatorGen: Gen[Person] = personEntities
  )(implicit renkuUrl: RenkuUrl): Gen[(Dataset[P], Dataset[Dataset.Provenance.Modified])] = for {
    original <- datasetEntities(provenance)(renkuUrl)(projectDateCreated)
    modified <- modifiedDatasetEntities(original, projectDateCreated, modificationCreatorGen)
  } yield original -> modified

  def modifiedDatasetEntities(
      original:           Dataset[Provenance],
      projectDateCreated: projects.DateCreated,
      creatorEntityGen:   Gen[Person] = personEntities
  )(implicit renkuUrl: RenkuUrl): Gen[Dataset[Dataset.Provenance.Modified]] = for {
    identifier <- datasetIdentifiers
    title      <- datasetTitles
    date <- datasetCreatedDates(
              List(original.provenance.date.instant, projectDateCreated.value).max
            )
    modifyingPerson <- creatorEntityGen
    additionalInfo  <- datasetAdditionalInfos
    parts           <- datasetPartEntities(date.instant).toGeneratorOfList()
    publicationEventFactories <- publicationEventFactories {
                                   date match {
                                     case dateCreated: datasets.DateCreated => dateCreated.value
                                     case _ => projectDateCreated.value
                                   }
                                 }.toGeneratorOfList()
  } yield Dataset
    .from(
      original.identification.copy(identifier = identifier, title = title),
      Provenance.Modified(
        Dataset.entityId(identifier),
        DerivedFrom(original.entityId),
        original.provenance.topmostDerivedFrom,
        original.provenance.originalIdentifier,
        date,
        (modifyingPerson :: original.provenance.creators).sortBy(_.name),
        maybeInvalidationTime = None
      ),
      additionalInfo,
      parts,
      publicationEventFactories,
      projectDateCreated
    )
    .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  val datasetIdentifications: Gen[Dataset.Identification] = for {
    identifier <- datasetIdentifiers
    title      <- datasetTitles
    name       <- datasetNames
  } yield Dataset.Identification(identifier, title, name)

  val provenanceInternal: ProvenanceGen[Dataset.Provenance.Internal] = provenanceInternal()
  def provenanceInternal(creatorsGen: Gen[Person] = personEntities): ProvenanceGen[Dataset.Provenance.Internal] =
    (identifier, projectDateCreated) =>
      implicit renkuUrl =>
        for {
          date     <- datasetCreatedDates(projectDateCreated.value)
          creators <- creatorsGen.toGeneratorOfNonEmptyList(max = 1)
        } yield Dataset.Provenance.Internal(Dataset.entityId(identifier),
                                            OriginalIdentifier(identifier),
                                            date,
                                            creators.sortBy(_.name)
        )

  val provenanceImportedExternal: ProvenanceGen[Dataset.Provenance.ImportedExternal] =
    provenanceImportedExternal(datasetExternalSameAs)

  def provenanceImportedExternal(
      sameAsGen:   Gen[ExternalSameAs] = datasetExternalSameAs,
      creatorsGen: Gen[Person] = personEntities
  ): ProvenanceGen[Dataset.Provenance.ImportedExternal] = (identifier, _) =>
    implicit renkuUrl =>
      for {
        date     <- datasetPublishedDates()
        sameAs   <- sameAsGen
        creators <- creatorsGen.toGeneratorOfNonEmptyList(max = 1)
      } yield Dataset.Provenance.ImportedExternal(Dataset.entityId(identifier),
                                                  sameAs,
                                                  OriginalIdentifier(identifier),
                                                  date,
                                                  creators.sortBy(_.name)
      )

  val provenanceImportedInternalAncestorExternal: ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorExternal] =
    provenanceImportedInternalAncestorExternal()
  def provenanceImportedInternalAncestorExternal(
      creatorsGen: Gen[Person] = personEntities
  ): ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorExternal] =
    (identifier, _) =>
      implicit renkuUrl =>
        for {
          date       <- datasetPublishedDates()
          sameAs     <- datasetInternalSameAs
          originalId <- oneOf(fixed(OriginalIdentifier(identifier)), datasetOriginalIdentifiers)
          creators   <- creatorsGen.toGeneratorOfNonEmptyList(max = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorExternal(Dataset.entityId(identifier),
                                                                    sameAs,
                                                                    TopmostSameAs(sameAs),
                                                                    originalId,
                                                                    date,
                                                                    creators.sortBy(_.name)
        )

  def provenanceImportedInternalAncestorInternal(
      sameAsGen:   Gen[InternalSameAs] = datasetInternalSameAs,
      creatorsGen: Gen[Person] = personEntities
  ): ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorInternal] =
    (identifier, projectDateCreated) =>
      implicit renkuUrl =>
        for {
          date       <- datasetCreatedDates(projectDateCreated.value)
          sameAs     <- sameAsGen
          originalId <- oneOf(fixed(OriginalIdentifier(identifier)), datasetOriginalIdentifiers)
          creators   <- creatorsGen.toGeneratorOfNonEmptyList(max = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorInternal(Dataset.entityId(identifier),
                                                                    sameAs,
                                                                    TopmostSameAs(sameAs),
                                                                    originalId,
                                                                    date,
                                                                    creators.sortBy(_.name)
        )

  def provenanceImportedInternalAncestorInternal(
      sameAs:        InternalSameAs,
      topmostSameAs: TopmostSameAs,
      creatorsGen:   Gen[Person]
  ): ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorInternal] =
    (identifier, projectDateCreated) =>
      implicit renkuUrl =>
        for {
          date     <- datasetCreatedDates(projectDateCreated.value)
          creators <- creatorsGen.toGeneratorOfNonEmptyList(max = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorInternal(Dataset.entityId(identifier),
                                                                    sameAs,
                                                                    topmostSameAs,
                                                                    OriginalIdentifier(identifier),
                                                                    date,
                                                                    creators.sortBy(_.name)
        )
  val provenanceImportedInternal: ProvenanceGen[Dataset.Provenance.ImportedInternal] =
    provenanceImportedInternal()
  def provenanceImportedInternal(
      personEntitiesGen: Gen[Person] = personEntities
  ): ProvenanceGen[Dataset.Provenance.ImportedInternal] = Random.shuffle {
    List(
      provenanceImportedInternalAncestorExternal(personEntitiesGen)
        .asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]],
      provenanceImportedInternalAncestorInternal(creatorsGen = personEntitiesGen)
        .asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]]
    )
  }.head

  val provenanceNonModified: ProvenanceGen[Dataset.Provenance.NonModified] = provenanceNonModified()
  def provenanceNonModified(creatorsGen: Gen[Person] = personEntities): ProvenanceGen[Dataset.Provenance.NonModified] =
    Random.shuffle {
      List(
        provenanceInternal(creatorsGen = creatorsGen).asInstanceOf[ProvenanceGen[Dataset.Provenance.NonModified]],
        provenanceImportedExternal(creatorsGen = creatorsGen)
          .asInstanceOf[ProvenanceGen[Dataset.Provenance.NonModified]],
        provenanceImportedInternal(personEntitiesGen = creatorsGen)
          .asInstanceOf[ProvenanceGen[Dataset.Provenance.NonModified]]
      )
    }.head

  val datasetAdditionalInfos: Gen[Dataset.AdditionalInfo] = for {
    maybeDescription <- datasetDescriptions.toGeneratorOfOptions
    keywords         <- datasetKeywords.toGeneratorOfList()
    images           <- imageUris.toGeneratorOfList()
    maybeLicense     <- datasetLicenses.toGeneratorOfOptions
    maybeVersion     <- datasetVersions.toGeneratorOfOptions
  } yield Dataset.AdditionalInfo(maybeDescription, keywords, images, maybeLicense, maybeVersion)

  def datasetPartEntities(minDateCreated: Instant): Gen[DatasetPart] = for {
    external    <- datasetPartExternals
    entity      <- inputEntities
    dateCreated <- datasetCreatedDates(minDateCreated)
    maybeSource <- datasetPartSources.toGeneratorOfOptions
  } yield DatasetPart(datasetPartIds.generateOne, external, entity, dateCreated, maybeSource)

  lazy val publicationEventFactories: Instant => Gen[Dataset[Provenance] => PublicationEvent] = minDateCreated =>
    for {
      maybeDescription <- sentences().map(_.value).map(publicationEvents.Description.apply).toGeneratorOfOptions
      name             <- nonBlankStrings(minLength = 5).map(_.value).map(publicationEvents.Name.apply)
      startDate        <- timestampsNotInTheFuture(minDateCreated) map publicationEvents.StartDate.apply
    } yield ds => PublicationEvent(ds, maybeDescription, name, startDate)

  lazy val publicationEventEntities: Dataset[Provenance] => Gen[PublicationEvent] = ds =>
    publicationEventFactories(ds.provenance.date.instant).map(_.apply(ds))

  lazy val publicationEventFactory: Dataset[Provenance] => PublicationEvent = ds =>
    publicationEventFactories(ds.provenance.date.instant).map(_.apply(ds)).generateOne

  implicit class DatasetGenFactoryOps[P <: Dataset.Provenance](factory: DatasetGenFactory[P])(implicit
      renkuUrl: RenkuUrl
  ) {

    lazy val decoupledFromProject: Gen[Dataset[P]] = factory(projectCreatedDates().generateOne)

    def generateList(projectDateCreated: projects.DateCreated): List[Dataset[P]] =
      factory(projectDateCreated).generateList()

    def multiple: List[DatasetGenFactory[P]] = List.fill(positiveInts(5).generateOne)(factory)

    def createMultiple(max: Int): List[DatasetGenFactory[P]] = List.fill(Random.nextInt(max - 1) + 1)(factory)

    def createModification: DatasetGenFactory[Dataset.Provenance.Modified] =
      dateCreated => Kleisli(factory).flatMap(ds => Kleisli(ds.createModification())).run(dateCreated)

    def toGeneratorFor(project: RenkuProject): Gen[Dataset[P]] = factory(project.dateCreated)

    def withDateAfter(projectDate: projects.DateCreated): Gen[Dataset[P]] = factory(projectDate)

    def withDateBefore(
        max: InstantTinyType
    )(implicit dsDate: Lens[P, InstantTinyType]): Gen[Dataset[P]] =
      factory(projects.DateCreated(max.value))
        .map(ds => ds.copy(provenance = dsDate.modify(_ => max)(ds.provenance)))
        .map(ds =>
          ds.copy(parts = ds.parts.map {
            case part if (part.dateCreated.instant compareTo ds.provenance.date.instant) < 0 =>
              part.copy(dateCreated = datasets.DateCreated(ds.provenance.date.instant))
            case part => part
          })
        )

    def modify[PP <: Provenance](f: Dataset[P] => Dataset[PP]): DatasetGenFactory[PP] =
      projectCreationDate => factory(projectCreationDate).map(f)
  }

  implicit def identificationLens[P <: Dataset.Provenance]: Lens[Dataset[P], Identification] =
    Lens[Dataset[P], Identification](_.identification)(identification => ds => ds.copy(identification = identification))

  implicit def provenanceLens[P <: Dataset.Provenance]: Lens[Dataset[P], P] =
    Lens[Dataset[P], P](_.provenance)(prov => ds => ds.copy(provenance = prov))

  implicit def additionalInfoLens[P <: Dataset.Provenance]: Lens[Dataset[P], AdditionalInfo] =
    Lens[Dataset[P], AdditionalInfo](_.additionalInfo)(additionalInfo => ds => ds.copy(additionalInfo = additionalInfo))

  implicit def creatorsLens[P <: Dataset.Provenance]: Lens[P, NonEmptyList[Person]] =
    Lens[P, NonEmptyList[Person]](_.creators) { crts =>
      {
        case p: Provenance.Internal                         => p.copy(creators = crts.sortBy(_.name)).asInstanceOf[P]
        case p: Provenance.ImportedExternal                 => p.copy(creators = crts.sortBy(_.name)).asInstanceOf[P]
        case p: Provenance.ImportedInternalAncestorExternal => p.copy(creators = crts.sortBy(_.name)).asInstanceOf[P]
        case p: Provenance.ImportedInternalAncestorInternal => p.copy(creators = crts.sortBy(_.name)).asInstanceOf[P]
        case p: Provenance.Modified                         => p.copy(creators = crts.sortBy(_.name)).asInstanceOf[P]
      }
    }

  def replaceDSName[P <: Dataset.Provenance](to: datasets.Name): Dataset[P] => Dataset[P] =
    identificationLens[P].modify(_.copy(name = to))

  def replaceDSDateCreatedOrPublished[P <: Dataset.Provenance](to: Instant): Dataset[P] => Dataset[P] = {
    val datePublished = datasets.DatePublished(to.atOffset(ZoneOffset.UTC).toLocalDate)
    val dateCreated   = datasets.DateCreated(to)
    provenanceLens[P].modify {
      case p: Provenance.Internal                         => p.copy(date = dateCreated).asInstanceOf[P]
      case p: Provenance.ImportedExternal                 => p.copy(date = datePublished).asInstanceOf[P]
      case p: Provenance.ImportedInternalAncestorExternal => p.copy(date = datePublished).asInstanceOf[P]
      case p: Provenance.ImportedInternalAncestorInternal => p.copy(date = dateCreated).asInstanceOf[P]
      case p: Provenance.Modified                         => p.copy(date = dateCreated).asInstanceOf[P]
    }
  }

  def replaceDSKeywords[P <: Dataset.Provenance](to: List[datasets.Keyword]): Dataset[P] => Dataset[P] =
    additionalInfoLens[P].modify(_.copy(keywords = to))

  def replaceDSDesc[P <: Dataset.Provenance](to: Option[datasets.Description]): Dataset[P] => Dataset[P] =
    additionalInfoLens[P].modify(_.copy(maybeDescription = to))

  implicit lazy val internalProvenanceDateLens: Lens[Dataset.Provenance.Internal, InstantTinyType] =
    Lens[Dataset.Provenance.Internal, InstantTinyType](_.date) { max =>
      _.copy(date = timestamps(max = max.value).generateAs[datasets.DateCreated])
    }
  implicit lazy val importedExternalProvenanceDateLens: Lens[Dataset.Provenance.ImportedExternal, InstantTinyType] =
    Lens[Dataset.Provenance.ImportedExternal, InstantTinyType](date =>
      new InstantTinyType { override def value = date.date.instant }
    ) { max =>
      _.copy(date =
        timestamps(max = max.value).map(LocalDate.ofInstant(_, ZoneOffset.UTC)).generateAs[datasets.DatePublished]
      )
    }
  implicit lazy val importedInternalAncestorExternalProvenanceDateLens
      : Lens[Dataset.Provenance.ImportedInternalAncestorExternal, InstantTinyType] =
    Lens[Dataset.Provenance.ImportedInternalAncestorExternal, InstantTinyType](date =>
      new InstantTinyType { override def value = date.date.instant }
    ) { max =>
      _.copy(date =
        timestamps(max = max.value).map(LocalDate.ofInstant(_, ZoneOffset.UTC)).generateAs[datasets.DatePublished]
      )
    }
  implicit lazy val importedInternalAncestorInternalProvenanceDateLens
      : Lens[Dataset.Provenance.ImportedInternalAncestorInternal, InstantTinyType] =
    Lens[Dataset.Provenance.ImportedInternalAncestorInternal, InstantTinyType](_.date) { max =>
      _.copy(date = timestamps(max = max.value).generateAs[datasets.DateCreated])
    }
  implicit lazy val modifiedProvenanceDateLens: Lens[Dataset.Provenance.Modified, InstantTinyType] =
    Lens[Dataset.Provenance.Modified, InstantTinyType](_.date) { max =>
      _.copy(date = timestamps(max = max.value).generateAs[datasets.DateCreated])
    }
}
