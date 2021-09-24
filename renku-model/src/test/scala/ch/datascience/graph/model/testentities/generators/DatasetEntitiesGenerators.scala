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
package generators

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{nonEmptyStrings, sentences, timestamps}
import ch.datascience.graph.model.GraphModelGenerators.{datasetCreatedDates, datasetDerivedFroms, datasetDescriptions, datasetExternalSameAs, datasetIdentifiers, datasetImageUris, datasetInitialVersions, datasetInternalSameAs, datasetKeywords, datasetLicenses, datasetNames, datasetPartExternals, datasetPartSources, datasetPublishedDates, datasetTitles, datasetVersions, projectCreatedDates}
import ch.datascience.graph.model._
import ch.datascience.graph.model.datasets.{DerivedFrom, ExternalSameAs, Identifier, InitialVersion, PartId, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.graph.model.testentities.Dataset.{AdditionalInfo, Identification, Provenance}
import ch.datascience.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory
import ch.datascience.tinytypes.InstantTinyType
import eu.timepit.refined.auto._
import monocle.Lens
import org.scalacheck.Gen

import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

trait DatasetEntitiesGenerators {
  self: EntitiesGenerators =>

  type ProvenanceGen[+P <: Provenance] = (Identifier, InstantTinyType) => RenkuBaseUrl => Gen[P]

  def datasetEntities[P <: Dataset.Provenance](
      provenanceGen:       ProvenanceGen[P],
      identificationGen:   Gen[Identification] = datasetIdentifications,
      additionalInfoGen:   Gen[AdditionalInfo] = datasetAdditionalInfos
  )(implicit renkuBaseUrl: RenkuBaseUrl): DatasetGenFactory[P] = projectDateCreated =>
    for {
      identification <- identificationGen
      provenance     <- provenanceGen(identification.identifier, projectDateCreated)(renkuBaseUrl)
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

  def modifiedDatasetEntities(
      original:            Dataset[Provenance]
  )(implicit renkuBaseUrl: RenkuBaseUrl): DatasetGenFactory[Provenance.Modified] = projectDateCreated =>
    for {
      identifier <- datasetIdentifiers
      title      <- datasetTitles
      date <- datasetCreatedDates(
                List(original.provenance.date.instant, projectDateCreated.value).max
              )
      modifyingPerson <- personEntities
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
          original.provenance.initialVersion,
          date,
          original.provenance.creators + modifyingPerson,
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

  val provenanceInternal: ProvenanceGen[Dataset.Provenance.Internal] = (identifier, projectDateCreated) =>
    implicit renkuBaseUrl =>
      for {
        date     <- datasetCreatedDates(projectDateCreated.value)
        creators <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.Internal(Dataset.entityId(identifier), InitialVersion(identifier), date, creators)

  val provenanceImportedExternal: ProvenanceGen[Dataset.Provenance.ImportedExternal] =
    provenanceImportedExternal(datasetExternalSameAs)

  def provenanceImportedExternal(
      sameAsGen: Gen[ExternalSameAs]
  ): ProvenanceGen[Dataset.Provenance.ImportedExternal] = (identifier, _) =>
    implicit renkuBaseUrl =>
      for {
        date     <- datasetPublishedDates()
        sameAs   <- sameAsGen
        creators <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.ImportedExternal(Dataset.entityId(identifier),
                                                  sameAs,
                                                  InitialVersion(identifier),
                                                  date,
                                                  creators
      )

  val provenanceImportedInternalAncestorExternal: ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorExternal] =
    (identifier, _) =>
      implicit renkuBaseUrl =>
        for {
          date           <- datasetPublishedDates()
          sameAs         <- datasetInternalSameAs
          initialVersion <- datasetInitialVersions
          creators       <- personEntities.toGeneratorOfSet(maxElements = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorExternal(Dataset.entityId(identifier),
                                                                    sameAs,
                                                                    TopmostSameAs(sameAs),
                                                                    initialVersion,
                                                                    date,
                                                                    creators
        )

  val provenanceImportedInternalAncestorInternal: ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorInternal] =
    (identifier, projectDateCreated) =>
      implicit renkuBaseUrl =>
        for {
          date     <- datasetCreatedDates(projectDateCreated.value)
          sameAs   <- datasetInternalSameAs
          creators <- personEntities.toGeneratorOfSet(maxElements = 1)
        } yield Dataset.Provenance.ImportedInternalAncestorInternal(Dataset.entityId(identifier),
                                                                    sameAs,
                                                                    TopmostSameAs(sameAs),
                                                                    InitialVersion(identifier),
                                                                    date,
                                                                    creators
        )

  val provenanceImportedInternal: ProvenanceGen[Dataset.Provenance.ImportedInternal] = Random.shuffle {
    List(
      provenanceImportedInternalAncestorExternal.asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]],
      provenanceImportedInternalAncestorInternal.asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]]
    )
  }.head

  val provenanceNonModified: ProvenanceGen[Dataset.Provenance.NonModified] = Random.shuffle {
    List(
      provenanceInternal.asInstanceOf[ProvenanceGen[Dataset.Provenance.NonModified]],
      provenanceImportedExternal.asInstanceOf[ProvenanceGen[Dataset.Provenance.NonModified]],
      provenanceImportedInternal.asInstanceOf[ProvenanceGen[Dataset.Provenance.NonModified]]
    )
  }.head

  val provenanceModified: ProvenanceGen[Dataset.Provenance.Modified] = (identifier, projectDateCreated) =>
    implicit renkuBaseUrl =>
      for {
        date        <- datasetCreatedDates(projectDateCreated.value)
        derivedFrom <- datasetDerivedFroms
        creators    <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.Modified(Dataset.entityId(identifier),
                                          derivedFrom,
                                          TopmostDerivedFrom(derivedFrom),
                                          InitialVersion(identifier),
                                          date,
                                          creators,
                                          maybeInvalidationTime = None
      )

  val ofAnyProvenance: ProvenanceGen[Dataset.Provenance] = (identifier, projectDateCreated) =>
    renkuBaseUrl =>
      Gen.oneOf(
        provenanceInternal(identifier, projectDateCreated)(renkuBaseUrl),
        provenanceImportedExternal(identifier, projectDateCreated)(renkuBaseUrl),
        provenanceImportedInternalAncestorExternal(identifier, projectDateCreated)(renkuBaseUrl),
        provenanceImportedInternalAncestorInternal(identifier, projectDateCreated)(renkuBaseUrl),
        provenanceModified(identifier, projectDateCreated)(renkuBaseUrl)
      )

  val datasetAdditionalInfos: Gen[Dataset.AdditionalInfo] = for {
    maybeDescription <- datasetDescriptions.toGeneratorOfOptions
    keywords         <- datasetKeywords.toGeneratorOfList()
    images           <- datasetImageUris.toGeneratorOfList()
    maybeLicense     <- datasetLicenses.toGeneratorOfOptions
    maybeVersion     <- datasetVersions.toGeneratorOfOptions
  } yield Dataset.AdditionalInfo(maybeDescription, keywords, images, maybeLicense, maybeVersion)

  def datasetPartEntities(minDateCreated: Instant): Gen[DatasetPart] = for {
    external    <- datasetPartExternals
    entity      <- inputEntities
    dateCreated <- datasetCreatedDates(minDateCreated)
    maybeSource <- datasetPartSources.toGeneratorOfOptions
  } yield DatasetPart(PartId.generate, external, entity, dateCreated, maybeSource)

  def publicationEventFactories(minDateCreated: Instant): Gen[Dataset[Provenance] => PublicationEvent] = for {
    maybeDescription <- sentences().map(_.value).map(publicationEvents.Description.apply).toGeneratorOfOptions
    name             <- nonEmptyStrings() map publicationEvents.Name.apply
    startDate        <- timestamps(minDateCreated, max = Instant.now()) map publicationEvents.StartDate.apply
  } yield PublicationEvent(_, maybeDescription, name, startDate)

  implicit class DatasetGenFactoryOps[P <: Dataset.Provenance](factory: DatasetGenFactory[P])(implicit
      renkuBaseUrl:                                                     RenkuBaseUrl
  ) {

    lazy val decoupledFromProject: Gen[Dataset[P]] = factory(projectCreatedDates().generateOne)

    def generateList(projectDateCreated: projects.DateCreated): List[Dataset[P]] =
      factory(projectDateCreated).generateList()

    def createMultiple(max: Int): List[DatasetGenFactory[P]] = List.fill(Random.nextInt(max - 1) + 1)(factory)

    def toGeneratorFor(project: Project): Gen[Dataset[P]] = factory(project.dateCreated)

    def withDateBefore(
        max:           InstantTinyType
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

  implicit def provenanceLens[P <: Dataset.Provenance]: Lens[Dataset[P], P] =
    Lens[Dataset[P], P](_.provenance)(prov => ds => ds.copy(provenance = prov))

  implicit def creatorsLens[P <: Dataset.Provenance]: Lens[P, Set[Person]] =
    Lens[P, Set[Person]](_.creators) { creators =>
      {
        case p: Provenance.Internal                         => p.copy(creators = creators).asInstanceOf[P]
        case p: Provenance.ImportedExternal                 => p.copy(creators = creators).asInstanceOf[P]
        case p: Provenance.ImportedInternalAncestorExternal => p.copy(creators = creators).asInstanceOf[P]
        case p: Provenance.ImportedInternalAncestorInternal => p.copy(creators = creators).asInstanceOf[P]
        case p: Provenance.Modified                         => p.copy(creators = creators).asInstanceOf[P]
      }
    }

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
