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

import Dataset.{AdditionalInfo, Identification, Provenance}
import Plan.CommandParameters
import Plan.CommandParameters.CommandParameterFactory
import cats.Applicative
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model._
import ch.datascience.graph.model.commandParameters.ParameterDefaultValue
import ch.datascience.graph.model.datasets.{Date, DerivedFrom, ExternalSameAs, Identifier, InitialVersion, PartId, TopmostSameAs}
import ch.datascience.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import ch.datascience.graph.model.entityModel.{Checksum, Location}
import ch.datascience.graph.model.parameterValues.ValueOverride
import ch.datascience.graph.model.projects.{ForksCount, Visibility}
import ch.datascience.graph.model.publicationEvents.AboutEvent
import ch.datascience.graph.model.testentities.Entity.{InputEntity, OutputEntity}
import ch.datascience.graph.model.users.{Email, GitLabId}
import ch.datascience.tinytypes.InstantTinyType
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.scalacheck.Gen

import java.nio.charset.StandardCharsets._
import java.time.Instant
import scala.util.Random

object EntitiesGenerators extends EntitiesGenerators

private object Instances {
  implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
  implicit val gitLabApiUrl: GitLabApiUrl = gitLabApiUrls.generateOne
}

trait EntitiesGenerators {
  implicit val renkuBaseUrl: RenkuBaseUrl = Instances.renkuBaseUrl
  implicit val gitLabApiUrl: GitLabApiUrl = Instances.gitLabApiUrl

  def invalidationTimes(min: InstantTinyType): Gen[InvalidationTime] = invalidationTimes(min.value)

  def invalidationTimes(min: Instant*): Gen[InvalidationTime] =
    timestamps(min = min.max, max = Instant.now()).toGeneratorOf(InvalidationTime)

  val activityIds:        Gen[Activity.Id]          = Gen.uuid.map(uuid => Activity.Id(uuid.toString))
  val activityStartTimes: Gen[activities.StartTime] = timestampsNotInTheFuture.map(activities.StartTime.apply)
  def activityStartTimes(after: InstantTinyType): Gen[activities.StartTime] =
    timestampsNotInTheFuture(after.value).map(activities.StartTime.apply)
  val activityOrders: Gen[activities.Order] = positiveInts(999999).map(_.value).map(activities.Order.apply)

  val entityFileLocations:   Gen[Location.File]   = relativePaths() map Location.File.apply
  val entityFolderLocations: Gen[Location.Folder] = relativePaths() map Location.Folder.apply
  val entityLocations:       Gen[Location]        = Gen.oneOf(entityFileLocations, entityFolderLocations)
  val entityChecksums:       Gen[Checksum]        = nonBlankStrings(40, 40).map(_.value).map(Checksum.apply)

  implicit val planNames: Gen[plans.Name] = nonBlankStrings().map(_.value).generateAs[plans.Name]
  implicit val planDescriptions: Gen[plans.Description] =
    sentences().map(_.value).generateAs[plans.Description]
  implicit val planCommands: Gen[plans.Command] = nonBlankStrings().map(_.value).generateAs[plans.Command]
  implicit val planProgrammingLanguages: Gen[plans.ProgrammingLanguage] =
    nonBlankStrings().map(_.value).generateAs[plans.ProgrammingLanguage]
  implicit val planSuccessCodes: Gen[plans.SuccessCode] =
    positiveInts().map(_.value).generateAs[plans.SuccessCode]

  implicit val commandParameterNames: Gen[commandParameters.Name] =
    nonBlankStrings().map(_.value).generateAs[commandParameters.Name]
  implicit val commandParameterTemporaries: Gen[commandParameters.Temporary] =
    Gen.oneOf(commandParameters.Temporary.temporary, commandParameters.Temporary.nonTemporary)
  implicit val commandParameterEncodingFormats: Gen[commandParameters.EncodingFormat] =
    Gen
      .oneOf(UTF_8.name(), US_ASCII.name(), ISO_8859_1.name, UTF_16.name(), nonEmptyStrings().generateOne)
      .map(commandParameters.EncodingFormat(_))
  implicit val commandParameterFolderCreation: Gen[commandParameters.FolderCreation] =
    Gen.oneOf(commandParameters.FolderCreation.yes, commandParameters.FolderCreation.no)

  lazy val visibilityPublic:    Gen[Visibility] = fixed(Visibility.Public)
  lazy val visibilityNonPublic: Gen[Visibility] = Gen.oneOf(Visibility.Internal, Visibility.Private)
  lazy val visibilityAny:       Gen[Visibility] = projectVisibilities

  lazy val anyProjectEntities: Gen[Project[ForksCount]] = Gen.oneOf(
    projectEntities(visibilityAny)(anyForksCount),
    projectWithParentEntities(visibilityAny)
  )

  def projectWithParentEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH)
  ): Gen[ProjectWithParent[ForksCount.Zero]] =
    projectEntities[ForksCount.Zero](visibilityGen, minDateCreated).map(_.forkOnce()._2)

  def projectEntities[FC <: ForksCount](
      visibilityGen:        Gen[Visibility],
      minDateCreated:       projects.DateCreated = projects.DateCreated(Instant.EPOCH)
  )(implicit forksCountGen: Gen[FC]): Gen[ProjectWithoutParent[FC]] = for {
    path         <- projectPaths
    name         <- projectNames
    agent        <- cliVersions
    dateCreated  <- projectCreatedDates(minDateCreated.value)
    maybeCreator <- personEntities(withGitLabId).toGeneratorOfOptions
    visibility   <- visibilityGen
    members      <- personEntities(withGitLabId).toGeneratorOfSet(minElements = 0)
    version      <- projectSchemaVersions
    forksCount   <- forksCountGen
  } yield ProjectWithoutParent(path,
                               name,
                               agent,
                               dateCreated,
                               maybeCreator,
                               visibility,
                               forksCount,
                               members ++ maybeCreator,
                               version
  )

  implicit val zeroForksProject: Gen[ForksCount.Zero] = Gen.const(ForksCount.Zero)
  implicit val nonZeroForksProject: Gen[ForksCount.NonZero] =
    positiveInts(max = 100) map ForksCount.apply
  val anyForksCount: Gen[ForksCount] = Gen.oneOf(zeroForksProject, nonZeroForksProject)
  def fixedForksCount(count: Int Refined Positive): Gen[ForksCount.NonZero] = ForksCount(count)

  implicit lazy val gitLabProjectInfos: Gen[GitLabProjectInfo] = for {
    name            <- projectNames
    path            <- projectPaths
    dateCreated     <- projectCreatedDates()
    maybeCreator    <- projectMemberObjects.toGeneratorOfOptions
    members         <- projectMemberObjects.toGeneratorOfSet()
    visibility      <- projectVisibilities
    maybeParentPath <- projectPaths.toGeneratorOfOptions
  } yield GitLabProjectInfo(name, path, dateCreated, maybeCreator, members, visibility, maybeParentPath)

  implicit lazy val projectMemberObjects: Gen[ProjectMember] = for {
    name     <- userNames
    username <- usernames
    gitLabId <- userGitLabIds
  } yield ProjectMember(name, username, gitLabId)

  val datasetIdentifications: Gen[Dataset.Identification] = for {
    identifier <- datasetIdentifiers
    title      <- datasetTitles
    name       <- datasetNames
  } yield Dataset.Identification(identifier, title, name)

  type ProvenanceGen[+P <: Provenance] = (Identifier, projects.DateCreated) => RenkuBaseUrl => Gen[P]

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

  val datasetProvenanceImportedInternal: ProvenanceGen[Dataset.Provenance.ImportedInternal] = Random.shuffle {
    List(
      datasetProvenanceImportedInternalAncestorExternal
        .asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]],
      datasetProvenanceImportedInternalAncestorInternal
        .asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]]
    )
  }.head

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
      projectGen:          Gen[Project[ForksCount.Zero]] = projectEntities(visibilityPublic)
  )(implicit renkuBaseUrl: RenkuBaseUrl): Gen[List[Dataset[Dataset.Provenance.ImportedExternal]]] =
    for {
      dataset <- datasetEntities(datasetProvenanceImportedExternal, projectGen)
    } yield (1 until sharedInProjects).foldLeft(List(dataset)) { (datasets, _) =>
      val identifier = datasetIdentifiers.generateOne
      dataset.copy(
        identification = dataset.identification.copy(identifier = identifier),
        provenance = Dataset.Provenance.ImportedExternal(Dataset.entityId(identifier),
                                                         dataset.provenance.sameAs,
                                                         InitialVersion(identifier),
                                                         dataset.provenance.date,
                                                         dataset.provenance.creators
        ),
        project = projectGen.generateOne match {
          case proj: ProjectWithParent[_] =>
            proj.copy(
              dateCreated = timestamps(max = dataset.project.dateCreated.value).generateAs[projects.DateCreated]
            )
          case proj: ProjectWithoutParent[_] =>
            proj.copy(
              dateCreated = timestamps(max = dataset.project.dateCreated.value).generateAs[projects.DateCreated]
            )
        }
      ) :: datasets
    }

  def fixed[V](value: V): Gen[V] = Gen.const(value)

  def datasetEntities[P <: Dataset.Provenance](
      provenanceGen:       ProvenanceGen[P],
      projectsGen:         Gen[Project[ForksCount]] = projectEntities[ForksCount.Zero](visibilityPublic),
      identificationGen:   Gen[Identification] = datasetIdentifications,
      additionalInfoGen:   Gen[AdditionalInfo] = datasetAdditionalInfos
  )(implicit renkuBaseUrl: RenkuBaseUrl): Gen[Dataset[P]] = for {
    project        <- projectsGen
    identification <- identificationGen
    provenance     <- provenanceGen(identification.identifier, project.dateCreated)(renkuBaseUrl)
    additionalInfo <- additionalInfoGen
    publishing     <- datasetPublishing(provenance.date, project)
    parts          <- datasetPartEntities(provenance.date.instant).toGeneratorOfList()
  } yield Dataset(
    identification,
    provenance,
    additionalInfo,
    publishing,
    parts,
    project
  )

  def modifiedDatasetEntities[P <: Provenance](
      original:            Dataset[P]
  )(implicit renkuBaseUrl: RenkuBaseUrl): Gen[Dataset[Provenance.Modified]] = for {
    identifier <- datasetIdentifiers
    title      <- datasetTitles
    date <- datasetCreatedDates(
              List(original.provenance.date.instant, original.project.dateCreated.value).max
            )
    modifyingPerson <- personEntities
    additionalInfo  <- datasetAdditionalInfos
    publishing      <- datasetPublishing(date, original.project)
    parts           <- datasetPartEntities(date.instant).toGeneratorOfList()
  } yield Dataset(
    original.identification.copy(identifier = identifier, title = title),
    Provenance.Modified(
      Dataset.entityId(identifier),
      DerivedFrom(original.entityId),
      original.provenance.topmostDerivedFrom,
      original.provenance.initialVersion,
      date,
      original.provenance.creators + modifyingPerson
    ),
    additionalInfo,
    publishing,
    parts,
    original.project
  )

  def datasetPartEntities(minDateCreated: Instant): Gen[DatasetPart] = for {
    external    <- datasetPartExternals
    entity      <- inputEntities
    dateCreated <- datasetCreatedDates(minDateCreated)
    maybeUrl    <- datasetUrls.toGeneratorOfOptions
    maybeSource <- datasetPartSources.toGeneratorOfOptions
  } yield DatasetPart(PartId.generate, external, entity, dateCreated, maybeUrl, maybeSource)

  def publicationEventEntities(minDateCreated: Instant): Gen[PublicationEvent] = for {
    about            <- nonEmptyStrings() map AboutEvent.apply
    maybeDescription <- sentences().map(_.value).map(publicationEvents.Description.apply).toGeneratorOfOptions
    location         <- relativePaths() map publicationEvents.Location.apply
    name             <- nonEmptyStrings() map publicationEvents.Name.apply
    startDate        <- timestamps(minDateCreated, max = Instant.now()) map publicationEvents.StartDate.apply
  } yield PublicationEvent(about, maybeDescription, location, name, startDate)

  lazy val inputEntities: Gen[InputEntity] = for {
    location <- entityLocations
    checksum <- entityChecksums
  } yield InputEntity(location, checksum)

  lazy val outputEntityFactories: Gen[Generation => OutputEntity] = for {
    location <- entityLocations
    checksum <- entityChecksums
  } yield (generation: Generation) => OutputEntity(location, checksum, generation)

  implicit val agentEntities: Gen[Agent] = cliVersions map Agent.apply

  lazy val withGitLabId:    Gen[Option[GitLabId]] = userGitLabIds.toGeneratorOfSomes
  lazy val withoutGitLabId: Gen[Option[GitLabId]] = fixed(Option.empty[GitLabId])
  lazy val withEmail:       Gen[Option[Email]]    = userEmails.toGeneratorOfSomes
  lazy val withoutEmail:    Gen[Option[Email]]    = userEmails.toGeneratorOfNones

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

  lazy val parameterDefaultValues: Gen[ParameterDefaultValue] =
    nonBlankStrings().map(v => ParameterDefaultValue(v.value))

  def planEntities(parameterFactories: CommandParameterFactory*): Project[ForksCount] => Gen[Plan] = project =>
    for {
      name    <- planNames
      command <- planCommands
    } yield Plan(name, command, CommandParameters.of(parameterFactories: _*), project)

  lazy val parameterValueOverrides: Gen[ValueOverride] =
    nonBlankStrings().map(v => ValueOverride(v.value))

  def executionPlanners(planGen:    Project[ForksCount] => Gen[Plan],
                        projectGen: Gen[Project[ForksCount]] = projectEntities[ForksCount.Zero](visibilityAny)
  ): Gen[ExecutionPlanner] = for {
    project    <- projectGen
    plan       <- planGen(project)
    author     <- personEntities
    cliVersion <- cliVersions
  } yield ExecutionPlanner.of(plan, activityStartTimes(project.dateCreated).generateOne, author, cliVersion)

  private implicit lazy val genApplicative: Applicative[Gen] = new Applicative[Gen] {
    override def pure[A](x:   A): Gen[A] = Gen.const(x)
    override def ap[A, B](ff: Gen[A => B])(fa: Gen[A]): Gen[B] = fa.flatMap(a => ff.map(f => f(a)))
  }
}
