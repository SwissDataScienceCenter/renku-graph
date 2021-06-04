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

import cats.Applicative
import ch.datascience.generators.CommonGraphGenerators.{cliVersions, gitLabApiUrls, renkuBaseUrls}
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Date, DerivedFrom, ExternalSameAs, Identifier, InitialVersion, PartId, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.graph.model.users.{Email, GitLabId}
import ch.datascience.graph.model.{datasets, projects}
import ch.datascience.rdfstore.entities.Activity.Order
import ch.datascience.rdfstore.entities.Dataset.{AdditionalInfo, Identification, Provenance}
import ch.datascience.rdfstore.entities.Entity.{Checksum, InputEntity}
import ch.datascience.rdfstore.entities.Project.ForksCount
import ch.datascience.rdfstore.entities.PublicationEvent.AboutEvent
import ch.datascience.rdfstore.entities.RunPlan.CommandParameters
import ch.datascience.rdfstore.entities.RunPlan.CommandParameters.CommandParameterFactory
import ch.datascience.tinytypes.InstantTinyType
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.scalacheck.Gen

import java.time.Instant

object EntitiesGenerators extends EntitiesGenerators

trait EntitiesGenerators {
  implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
  implicit val gitLabApiUrl: GitLabApiUrl = gitLabApiUrls.generateOne

  val activityIds:        Gen[Activity.Id]        = Gen.uuid.map(uuid => Activity.Id(uuid.toString))
  val activityStartTimes: Gen[Activity.StartTime] = timestampsNotInTheFuture.map(Activity.StartTime.apply)
  def activityStartTimes(after: InstantTinyType): Gen[Activity.StartTime] =
    timestampsNotInTheFuture(after.value).map(Activity.StartTime.apply)
  val activityOrders: Gen[Activity.Order] = positiveInts(999999).map(_.value).map(Order.apply)

  val entityFileLocations:   Gen[Location.File]   = relativePaths() map Location.File.apply
  val entityFolderLocations: Gen[Location.Folder] = relativePaths() map Location.Folder.apply
  val entityLocations:       Gen[Location]        = Gen.oneOf(entityFileLocations, entityFolderLocations)
  val entityChecksums:       Gen[Checksum]        = nonBlankStrings(40, 40).map(_.value).map(Checksum.apply)

  implicit val runPlanNames: Gen[RunPlan.Name] = nonBlankStrings().map(_.value).generateAs[RunPlan.Name]
  implicit val runPlanDescriptions: Gen[RunPlan.Description] =
    sentences().map(_.value).generateAs[RunPlan.Description]
  implicit val runPlanCommands: Gen[RunPlan.Command] = nonBlankStrings().map(_.value).generateAs[RunPlan.Command]
  implicit val runPlanProgrammingLanguages: Gen[RunPlan.ProgrammingLanguage] =
    nonBlankStrings().map(_.value).generateAs[RunPlan.ProgrammingLanguage]

  implicit val commandParameterNames: Gen[CommandParameterBase.Name] =
    nonBlankStrings().map(_.value).generateAs[CommandParameterBase.Name]

  lazy val visibilityPublic:    Gen[Visibility] = fixed(Visibility.Public)
  lazy val visibilityNonPublic: Gen[Visibility] = Gen.oneOf(Visibility.Internal, Visibility.Private)
  lazy val visibilityAny:       Gen[Visibility] = projectVisibilities

  def projectWitParentEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH)
  ): Gen[Project[ForksCount.Zero] with HavingParent] =
    projectEntities[ForksCount.Zero](visibilityGen, minDateCreated).map(_.forkOnce()._2)

  def projectEntities[FC <: Project.ForksCount](
      visibilityGen:        Gen[Visibility],
      minDateCreated:       projects.DateCreated = projects.DateCreated(Instant.EPOCH)
  )(implicit forksCountGen: Gen[FC]): Gen[Project[FC]] = for {
    path         <- projectPaths
    name         <- projectNames
    agent        <- cliVersions
    dateCreated  <- projectCreatedDates(minDateCreated.value)
    maybeCreator <- personEntities.toGeneratorOfOptions
    visibility   <- visibilityGen
    members      <- personEntities(userGitLabIds.toGeneratorOfSomes).toGeneratorOfSet(minElements = 0)
    version      <- projectSchemaVersions
    forksCount   <- forksCountGen
  } yield Project[FC](path, name, agent, dateCreated, maybeCreator, visibility, forksCount, members, version)

  implicit val zeroForksProject: Gen[Project.ForksCount.Zero] = Gen.const(Project.ForksCount.Zero)
  implicit val nonZeroForksProject: Gen[Project.ForksCount.NonZero] =
    positiveInts(max = 100) map Project.ForksCount.apply
  def fixedForksCount(count: Int Refined Positive): Gen[Project.ForksCount.NonZero] = Project.ForksCount(count)

  val datasetIdentifications: Gen[Dataset.Identification] = for {
    identifier <- datasetIdentifiers
    title      <- datasetTitles
    name       <- datasetNames
  } yield Dataset.Identification(identifier, title, name)

  type ProvenanceGen[P <: Provenance] = (Identifier, projects.DateCreated) => RenkuBaseUrl => Gen[P]

  val datasetProvenanceInternal: ProvenanceGen[Dataset.Provenance.Internal] = (identifier, projectDateCreated) =>
    implicit renkuBaseUrl =>
      for {
        date     <- datasetCreatedDates(projectDateCreated.value)
        creators <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.Internal(Dataset.entityId(identifier), InitialVersion(identifier), date, creators)

  val datasetProvenanceImportedExternal: ProvenanceGen[Dataset.Provenance.ImportedExternal] =
    datasetProvenanceImportedExternal(datasetExternalSameAs)

  def datasetProvenanceImportedExternal(
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

  val datasetProvenanceImportedInternalAncestorExternal
      : ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorExternal] = (identifier, _) =>
    implicit renkuBaseUrl =>
      for {
        date           <- datasetPublishedDates()
        sameAs         <- datasetInternalSameAs
        topmostSameAs  <- datasetExternalSameAs.map(TopmostSameAs(_))
        initialVersion <- datasetInitialVersions
        creators       <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.ImportedInternalAncestorExternal(Dataset.entityId(identifier),
                                                                  sameAs,
                                                                  topmostSameAs,
                                                                  initialVersion,
                                                                  date,
                                                                  creators
      )

  val datasetProvenanceImportedInternalAncestorInternal
      : ProvenanceGen[Dataset.Provenance.ImportedInternalAncestorInternal] = (identifier, projectDateCreated) =>
    implicit renkuBaseUrl =>
      for {
        date          <- datasetCreatedDates(projectDateCreated.value)
        sameAs        <- datasetInternalSameAs
        topmostSameAs <- datasetInternalSameAs
        creators      <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.ImportedInternalAncestorInternal(Dataset.entityId(identifier),
                                                                  sameAs,
                                                                  TopmostSameAs(topmostSameAs),
                                                                  InitialVersion(topmostSameAs.asIdentifier),
                                                                  date,
                                                                  creators
      )

  val datasetProvenanceModified: ProvenanceGen[Dataset.Provenance.Modified] = (identifier, projectDateCreated) =>
    implicit renkuBaseUrl =>
      for {
        date               <- datasetCreatedDates(projectDateCreated.value)
        derivedFrom        <- datasetDerivedFroms
        topmostDerivedFrom <- datasetTopmostDerivedFroms
        creators           <- personEntities.toGeneratorOfSet(maxElements = 1)
      } yield Dataset.Provenance.Modified(Dataset.entityId(identifier),
                                          derivedFrom,
                                          topmostDerivedFrom,
                                          InitialVersion(identifier),
                                          date,
                                          creators
      )

  val ofAnyProvenance: ProvenanceGen[Dataset.Provenance] = (identifier, projectDateCreated) =>
    renkuBaseUrl =>
      Gen.oneOf(
        datasetProvenanceInternal(identifier, projectDateCreated)(renkuBaseUrl),
        datasetProvenanceImportedExternal(identifier, projectDateCreated)(renkuBaseUrl),
        datasetProvenanceImportedInternalAncestorExternal(identifier, projectDateCreated)(renkuBaseUrl),
        datasetProvenanceImportedInternalAncestorInternal(identifier, projectDateCreated)(renkuBaseUrl),
        datasetProvenanceModified(identifier, projectDateCreated)(renkuBaseUrl)
      )

  def datasetPublishing(date: Date, project: Project[_]): Gen[Dataset.Publishing] = for {
    publicationEvents <-
      publicationEventEntities(date match {
        case dateCreated: datasets.DateCreated => dateCreated.value
        case _ => project.dateCreated.value
      }).toGeneratorOfList()
    maybeVersion <- datasetVersions.toGeneratorOfOptions
  } yield Dataset.Publishing(publicationEvents, maybeVersion)

  val datasetAdditionalInfos: Gen[Dataset.AdditionalInfo] = for {
    url              <- datasetUrls
    maybeDescription <- datasetDescriptions.toGeneratorOfOptions
    keywords         <- datasetKeywords.toGeneratorOfList()
    images           <- datasetImageUris.toGeneratorOfList()
    maybeLicense     <- datasetLicenses.toGeneratorOfOptions
  } yield Dataset.AdditionalInfo(url, maybeDescription, keywords, images, maybeLicense)

  def importedExternalDatasetEntities(
      sharedInProjects:    Int = 1,
      projectGen:          Gen[Project[Project.ForksCount.Zero]] = projectEntities(visibilityPublic)
  )(implicit renkuBaseUrl: RenkuBaseUrl): Gen[List[Dataset[Dataset.Provenance.ImportedExternal]]] =
    for {
      dataset <- datasetEntities(datasetProvenanceImportedExternal, projectGen)
    } yield (1 until sharedInProjects).foldLeft(List(dataset)) { (datasets, _) =>
      datasets :+ dataset.copy(
        identification = dataset.identification.copy(identifier = datasetIdentifiers.generateOne),
        project = projectGen.generateOne
      )
    }

  def fixed[V](value: V): Gen[V] = Gen.const(value)

  def datasetEntities[P <: Dataset.Provenance](
      provenanceGen:       ProvenanceGen[P],
      projectsGen:         Gen[Project[Project.ForksCount]] = projectEntities[ForksCount.Zero](visibilityPublic),
      identificationGen:   Gen[Identification] = datasetIdentifications,
      additionalInfoGen:   Gen[AdditionalInfo] = datasetAdditionalInfos
  )(implicit renkuBaseUrl: RenkuBaseUrl): Gen[Dataset[P]] = for {
    project        <- projectsGen
    identification <- identificationGen
    provenance     <- provenanceGen(identification.identifier, project.dateCreated)(renkuBaseUrl)
    additionalInfo <- additionalInfoGen
    publishing     <- datasetPublishing(provenance.date, project)
    parts          <- datasetPartEntities(provenance.date.instant).toGeneratorOfList()
    project        <- projectsGen
  } yield Dataset(
    identification,
    provenance,
    additionalInfo,
    publishing,
    parts,
    project
  )

  def modifiedDatasetEntities[P <: Provenance](
      original: Dataset[P]
  )(implicit
      findTopmostDerivedFrom: P => TopmostDerivedFrom,
      renkuBaseUrl:           RenkuBaseUrl
  ): Gen[Dataset[Provenance.Modified]] = for {
    identifier <- datasetIdentifiers
    date <- datasetCreatedDates(
              List(original.provenance.date.instant, original.project.dateCreated.value).sorted.reverse.head
            )
    modifyingPerson <- personEntities
    additionalInfo  <- datasetAdditionalInfos
    publishing      <- datasetPublishing(date, original.project)
    parts           <- datasetPartEntities(date.instant).toGeneratorOfList()
  } yield Dataset(
    original.identification.copy(identifier = identifier),
    Provenance.Modified(
      Dataset.entityId(identifier),
      DerivedFrom(original.entityId),
      findTopmostDerivedFrom(original.provenance),
      InitialVersion(identifier),
      date,
      original.provenance.creators + modifyingPerson
    ),
    additionalInfo,
    publishing,
    parts,
    original.project
  )

  implicit lazy val topmostDerivedFromInternal: Provenance.Internal => TopmostDerivedFrom =
    prov => TopmostDerivedFrom(prov.entityId)
  implicit lazy val topmostDerivedFromImportedExternal: Provenance.ImportedExternal => TopmostDerivedFrom =
    prov => TopmostDerivedFrom(prov.entityId)
  implicit lazy val topmostDerivedFromImportedInternal: Provenance.ImportedInternal => TopmostDerivedFrom =
    prov => TopmostDerivedFrom(prov.entityId)
  implicit lazy val topmostDerivedFromModified: Provenance.Modified => TopmostDerivedFrom =
    prov => prov.topmostDerivedFrom

  def datasetPartEntities(minDateCreated: Instant): Gen[DatasetPart] = for {
    external    <- datasetPartExternals
    entity      <- inputEntities
    dateCreated <- datasetCreatedDates(minDateCreated)
    maybeUrl    <- datasetUrls.toGeneratorOfOptions
    maybeSource <- datasetPartSources.toGeneratorOfOptions
  } yield DatasetPart(PartId.generate, external, entity, dateCreated, maybeUrl, maybeSource)

  def publicationEventEntities(minDateCreated: Instant): Gen[PublicationEvent] = for {
    about            <- nonEmptyStrings() map AboutEvent.apply
    maybeDescription <- sentences().map(_.value).map(PublicationEvent.Description.apply).toGeneratorOfOptions
    location         <- relativePaths() map PublicationEvent.Location.apply
    name             <- nonEmptyStrings() map PublicationEvent.Name.apply
    startDate        <- timestamps(minDateCreated, max = Instant.now()) map PublicationEvent.StartDate.apply
  } yield PublicationEvent(about, maybeDescription, location, name, startDate)

  lazy val inputEntities: Gen[Entity] = for {
    location <- entityLocations
    checksum <- entityChecksums
  } yield InputEntity(location, checksum)

  implicit val agentEntities: Gen[Agent] = cliVersions map Agent.apply

  lazy val withGitLabId:    Gen[Option[GitLabId]] = userGitLabIds.toGeneratorOfSomes
  lazy val withoutGitLabId: Gen[Option[GitLabId]] = fixed(Option.empty[GitLabId])
  lazy val withEmail:       Gen[Option[Email]]    = userEmails.toGeneratorOfSomes

  implicit lazy val personEntities: Gen[Person] = personEntities()

  def personEntities(
      maybeGitLabIds: Gen[Option[GitLabId]] = userGitLabIds.toGeneratorOfNones,
      maybeEmails:    Gen[Option[Email]] = userEmails.toGeneratorOfOptions
  ): Gen[Person] = for {
    name             <- userNames
    maybeEmail       <- maybeEmails
    maybeAffiliation <- userAffiliations.toGeneratorOfOptions
    maybeGitLabId    <- maybeGitLabIds
  } yield Person(name, maybeEmail, maybeAffiliation, maybeGitLabId)

  def runPlanEntities(parameterFactories: CommandParameterFactory*): Gen[RunPlan] = for {
    name    <- runPlanNames
    command <- runPlanCommands
  } yield RunPlan(name, command, CommandParameters.of(parameterFactories: _*))

  def executionPlanners(runPlanGen: Gen[RunPlan],
                        projectGen: Gen[Project[ForksCount]] = projectEntities[ForksCount.Zero](visibilityAny)
  ): Gen[ExecutionPlanner] = for {
    runPlan    <- runPlanGen
    project    <- projectGen
    author     <- personEntities
    cliVersion <- cliVersions
  } yield ExecutionPlanner.of(runPlan, activityStartTimes(project.dateCreated).generateOne, author, cliVersion, project)

  private implicit lazy val genApplicative: Applicative[Gen] = new Applicative[Gen] {
    override def pure[A](x:   A): Gen[A] = Gen.const(x)
    override def ap[A, B](ff: Gen[A => B])(fa: Gen[A]): Gen[B] = fa.flatMap(a => ff.map(f => f(a)))
  }
}
