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

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{fixed, nonNegativeInts, positiveInts}
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects.{ForksCount, Visibility}
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{ActivityGenFactory, DatasetGenFactory}
import io.renku.graph.model.{RenkuUrl, projects}
import org.scalacheck.Gen

import java.time.Instant
import scala.annotation.tailrec

trait RenkuProjectEntitiesGenerators {
  self: EntitiesGenerators =>

  lazy val visibilityPublic:    Gen[Visibility] = fixed(Visibility.Public)
  lazy val visibilityPrivate:   Gen[Visibility] = fixed(Visibility.Private)
  lazy val visibilityNonPublic: Gen[Visibility] = Gen.oneOf(Visibility.Internal, Visibility.Private)
  lazy val anyVisibility:       Gen[Visibility] = projectVisibilities

  lazy val anyRenkuProjectEntities: Gen[RenkuProject] = anyRenkuProjectEntities(anyVisibility)

  def anyRenkuProjectEntities(visibilityGen: Gen[Visibility],
                              creatorGen:    Gen[Person] = personEntities(withGitLabId)
  ): Gen[RenkuProject] = Gen.oneOf(
    renkuProjectEntities(visibilityGen, creatorGen = creatorGen),
    renkuProjectWithParentEntities(visibilityGen, creatorGen = creatorGen)
  )

  lazy val renkuProjectEntitiesWithDatasetsAndActivities: Gen[RenkuProject] =
    renkuProjectEntitiesWithDatasetsAndActivities()

  def renkuProjectEntitiesWithDatasetsAndActivities(personGen: Gen[Person] = personEntities): Gen[RenkuProject] =
    renkuProjectEntities(anyVisibility, creatorGen = personGen)
      .withActivities(
        List.fill(nonNegativeInts(max = 5).generateOne.value)(
          activityEntities(stepPlanEntities(planCommands, creatorsGen = personGen), authorGen = personGen)
        ): _*
      )
      .withDatasets(
        List.fill(nonNegativeInts(max = 5).generateOne.value)(datasetEntities(provenanceNonModified(personGen))): _*
      )

  def renkuProjectEntities(
      visibilityGen:         Gen[Visibility],
      projectDateCreatedGen: Gen[projects.DateCreated] = projectCreatedDates(Instant.EPOCH),
      creatorGen:            Gen[Person] = personEntities(withGitLabId),
      activityFactories:     List[ActivityGenFactory] = Nil,
      datasetFactories:      List[DatasetGenFactory[Dataset.Provenance]] = Nil,
      forksCountGen:         Gen[ForksCount] = anyForksCount
  ): Gen[RenkuProject.WithoutParent] = for {
    slug             <- projectSlugs
    name             <- Gen.const(slug.toName)
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    agent            <- cliVersions
    dateCreated      <- projectDateCreatedGen
    dateModified     <- projectModifiedDates(dateCreated.value)
    maybeCreator     <- creatorGen.toGeneratorOfOptions
    visibility       <- visibilityGen
    forksCount       <- forksCountGen
    keywords         <- projectKeywords.toGeneratorOfSet(min = 0)
    members          <- personEntities(withGitLabId).toGeneratorOfSet(min = 0)
    version          <- projectSchemaVersions
    activities       <- activityFactories.map(_.apply(dateCreated)).sequence
    datasets         <- datasetFactories.map(_.apply(dateCreated)).sequence
    images           <- imageUris.toGeneratorOfList()
  } yield RenkuProject.WithoutParent(
    slug,
    name,
    maybeDescription,
    agent,
    dateCreated,
    dateModified,
    maybeCreator,
    visibility,
    forksCount,
    keywords,
    members ++ maybeCreator,
    version,
    activities,
    datasets,
    images
  )

  def renkuProjectWithParentEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH),
      creatorGen:     Gen[Person] = personEntities(withGitLabId)
  ): Gen[RenkuProject.WithParent] =
    renkuProjectEntities(visibilityGen, minDateCreated, creatorGen = creatorGen).map(_.forkOnce(creatorGen)._2)

  implicit val forksCountZero:    Gen[ForksCount.Zero]    = Gen.const(ForksCount.Zero)
  implicit val forksCountNonZero: Gen[ForksCount.NonZero] = positiveInts(max = 100) map ForksCount.apply
  val anyForksCount:              Gen[ForksCount]         = Gen.oneOf(forksCountZero, forksCountNonZero)
  def fixedForksCount(count: Int Refined Positive): Gen[ForksCount.NonZero] = ForksCount(count)

  implicit lazy val gitLabProjectInfos: Gen[GitLabProjectInfo] = for {
    id               <- projectIds
    name             <- projectNames
    slug             <- projectSlugs
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    dateCreated      <- projectCreatedDates()
    dateModified     <- projectModifiedDates(dateCreated.value)
    maybeCreator     <- projectMembers.toGeneratorOfOptions
    keywords         <- projectKeywords.toGeneratorOfSet(min = 0)
    members          <- projectMembers.toGeneratorOfList(min = 1).map(_.toSet)
    visibility       <- projectVisibilities
    maybeParentSlug  <- projectSlugs.toGeneratorOfOptions
    avatarUri        <- imageUris.toGeneratorOfOptions
  } yield GitLabProjectInfo(id,
                            name,
                            slug,
                            dateCreated,
                            dateModified,
                            maybeDescription,
                            maybeCreator,
                            keywords,
                            members,
                            visibility,
                            maybeParentSlug,
                            avatarUri
  )

  implicit lazy val projectMembersNoEmail: Gen[ProjectMemberNoEmail] = for {
    name     <- personNames
    username <- personUsernames
    gitLabId <- personGitLabIds
  } yield ProjectMemberNoEmail(name, username, gitLabId)

  implicit lazy val projectMembersWithEmail: Gen[ProjectMemberWithEmail] = for {
    memberNoEmail <- projectMembersNoEmail
    email         <- personEmails
  } yield memberNoEmail add email

  lazy val projectMembers: Gen[ProjectMember] = Gen.oneOf(projectMembersNoEmail, projectMembersWithEmail)

  implicit class ProjectMemberGenOps(membersGen: Gen[ProjectMember]) {
    def modify(f: ProjectMember => ProjectMember): Gen[ProjectMember] = membersGen.map(f)
  }

  implicit class RenkuProjectGenFactoryOps(projectGen: Gen[RenkuProject])(implicit renkuUrl: RenkuUrl) {

    def withDatasets[P <: Dataset.Provenance](factories: DatasetGenFactory[P]*): Gen[RenkuProject] = for {
      project  <- projectGen
      datasets <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project match {
      case p: RenkuProject.WithParent    => p.addDatasets(datasets: _*)
      case p: RenkuProject.WithoutParent => p.addDatasets(datasets: _*)
    }

    def withActivities(factories: ActivityGenFactory*): Gen[RenkuProject] = for {
      project    <- projectGen
      activities <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project match {
      case p: RenkuProject.WithParent    => p.addActivities(activities: _*)
      case p: RenkuProject.WithoutParent => p.addActivities(activities: _*)
    }

    def addActivity(factory: RenkuProject => Gen[Activity]): Gen[RenkuProject] = for {
      project  <- projectGen
      activity <- factory(project)
    } yield project match {
      case p: RenkuProject.WithParent    => p.addActivities(activity)
      case p: RenkuProject.WithoutParent => p.addActivities(activity)
    }

    def addDataset[P <: Dataset.Provenance](factory: DatasetGenFactory[P]): Gen[(Dataset[P], RenkuProject)] = for {
      project <- projectGen
      ds      <- factory(project.dateCreated)
    } yield ds -> (project addDatasets ds)

    def addDatasetAndModification[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[((Dataset[P], Dataset[Dataset.Provenance.Modified]), RenkuProject)] = for {
      project    <- projectGen
      originalDs <- factory(project.dateCreated)
      modifiedDs <- originalDs.createModification()(project.dateCreated)
    } yield (originalDs -> modifiedDs) -> project.addDatasets(originalDs, modifiedDs)

    def addDatasetAndModifications(factory: DatasetGenFactory[Dataset.Provenance], level: Int): Gen[RenkuProject] = {

      @tailrec
      def createModifications(datasets: List[Dataset[Dataset.Provenance]], left: Int = level)(
          projectDateCreated: projects.DateCreated
      ): List[Dataset[Dataset.Provenance]] =
        if (left == 0) datasets
        else
          createModifications(datasets.head.createModification()(projectDateCreated).generateOne :: datasets, left - 1)(
            projectDateCreated
          )

      projectGen.modify { p =>
        val datasets = createModifications(List(factory(p.dateCreated).generateOne))(p.dateCreated)
        p.addDatasets(datasets.reverse: _*)
      }
    }

    def addDatasetAndInvalidation[P <: Dataset.Provenance](
        factory:    DatasetGenFactory[P],
        creatorGen: Gen[Person] = personEntities
    ): Gen[((Dataset[P], Dataset[Dataset.Provenance.Modified]), RenkuProject)] = for {
      project    <- projectGen
      originalDs <- factory(project.dateCreated)
      invalidated = originalDs.invalidateNow(creatorGen)
    } yield (originalDs -> invalidated) -> project.addDatasets(originalDs, invalidated)

    def importDataset[PIN <: Dataset.Provenance, POUT <: Dataset.Provenance](
        dataset: Dataset[PIN]
    )(implicit newProvenance: ProvenanceImportFactory[PIN, POUT]): Gen[(Dataset[POUT], RenkuProject)] =
      projectGen.map(_.importDataset(dataset))

    def importDataset(
        publicationEvent: PublicationEvent
    ): Gen[(Dataset[Dataset.Provenance.ImportedInternal], RenkuProject)] =
      projectGen.map(_.importDataset(publicationEvent))

    def modify(f: RenkuProject => RenkuProject): Gen[RenkuProject] = projectGen.map {
      case project: RenkuProject.WithoutParent => f(project)
      case project: RenkuProject.WithParent    => f(project)
    }

    def noEmailsOnMembers: Gen[RenkuProject] =
      modify(membersLens.modify(_.map(_.copy(maybeEmail = None))))
        .modify(creatorLens[RenkuProject].modify(_.map(_.copy(maybeEmail = None))))

    def forkOnce(): Gen[(RenkuProject, RenkuProject.WithParent)] = projectGen.map(_.forkOnce())
  }

  implicit class DatasetAndProjectOps[T](tupleGen: Gen[(T, RenkuProject)])(implicit renkuUrl: RenkuUrl) {

    def addDataset[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[((T, Dataset[P]), RenkuProject)] = for {
      (entities, project) <- tupleGen
      ds                  <- factory(project.dateCreated)
    } yield (entities -> ds) -> (project addDatasets ds)

    def addDatasetAndModification[P <: Dataset.Provenance](
        factory:    DatasetGenFactory[P],
        creatorGen: Gen[Person] = personEntities
    ): Gen[(((T, Dataset[P]), Dataset[Dataset.Provenance.Modified]), RenkuProject)] = for {
      (entities, project) <- tupleGen
      originalDs          <- factory(project.dateCreated)
      modifiedDs          <- originalDs.createModification(creatorGen = creatorGen)(project.dateCreated)
    } yield ((entities -> originalDs) -> modifiedDs) -> project.addDatasets(originalDs, modifiedDs)

    def addDatasetAndInvalidation[P <: Dataset.Provenance](
        factory:    DatasetGenFactory[P],
        creatorGen: Gen[Person] = personEntities
    ): Gen[(((T, Dataset[P]), Dataset[Dataset.Provenance.Modified]), RenkuProject)] = for {
      (entities, project) <- tupleGen
      originalDs          <- factory(project.dateCreated)
      invalidated = originalDs.invalidateNow(creatorGen)
    } yield ((entities -> originalDs) -> invalidated) -> project.addDatasets(originalDs, invalidated)

    def importDataset[PIN <: Dataset.Provenance, POUT <: Dataset.Provenance](
        dataset: Dataset[PIN]
    )(implicit newProvenance: ProvenanceImportFactory[PIN, POUT]): Gen[((T, Dataset[POUT]), RenkuProject)] =
      tupleGen map { case (entities, project) =>
        val (imported, updatedProject) = project.importDataset(dataset)
        ((entities, imported), updatedProject)
      }

    def forkOnce(creatorGen: Gen[Person] = personEntities): Gen[(T, (RenkuProject, RenkuProject.WithParent))] =
      tupleGen map { case (entities, project) => entities -> project.forkOnce(creatorGen) }
  }

  implicit class RenkuProjectWithParentGenFactoryOps(projectGen: Gen[RenkuProject.WithParent]) {

    def withDatasets[P <: Dataset.Provenance](factories: DatasetGenFactory[P]*): Gen[RenkuProject.WithParent] = for {
      project  <- projectGen
      datasets <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addDatasets(datasets: _*)

    def withActivities(factories: ActivityGenFactory*): Gen[RenkuProject.WithParent] = for {
      project    <- projectGen
      activities <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addActivities(activities: _*)

    def modify(f: RenkuProject.WithParent => RenkuProject.WithParent): Gen[RenkuProject.WithParent] =
      projectGen.map(f)
  }

  implicit class RenkuProjectWithoutParentGenFactoryOps(projectGen: Gen[RenkuProject.WithoutParent]) {

    def withDatasets[P <: Dataset.Provenance](factories: DatasetGenFactory[P]*): Gen[RenkuProject.WithoutParent] = for {
      project  <- projectGen
      datasets <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addDatasets(datasets: _*)

    def withActivities(factories: ActivityGenFactory*): Gen[RenkuProject.WithoutParent] = for {
      project    <- projectGen
      activities <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addActivities(activities: _*)

    def modify(f: RenkuProject.WithoutParent => RenkuProject.WithoutParent): Gen[RenkuProject.WithoutParent] =
      projectGen.map(f)
  }
}
