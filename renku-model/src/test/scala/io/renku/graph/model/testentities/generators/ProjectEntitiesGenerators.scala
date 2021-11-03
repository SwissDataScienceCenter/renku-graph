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

package io.renku.graph.model.testentities
package generators

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{fixed, nonNegativeInts, positiveInts}
import io.renku.graph.model.GraphModelGenerators.{cliVersions, projectCreatedDates, projectDescriptions, projectNames, projectPaths, projectSchemaVersions, projectVisibilities, userGitLabIds, userNames, usernames}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects.{ForksCount, Visibility}
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{ActivityGenFactory, DatasetGenFactory}
import io.renku.graph.model.{RenkuBaseUrl, projects}
import monocle.Lens
import org.scalacheck.Gen

import java.time.Instant

trait ProjectEntitiesGenerators {
  self: EntitiesGenerators =>

  lazy val visibilityPublic:    Gen[Visibility] = fixed(Visibility.Public)
  lazy val visibilityNonPublic: Gen[Visibility] = Gen.oneOf(Visibility.Internal, Visibility.Private)
  lazy val anyVisibility:       Gen[Visibility] = projectVisibilities

  lazy val anyProjectEntities: Gen[Project] = Gen.oneOf(
    projectEntities(anyVisibility),
    projectWithParentEntities(anyVisibility)
  )

  lazy val projectEntitiesWithDatasetsAndActivities: Gen[Project] =
    projectEntities(anyVisibility)
      .withActivities(
        List.fill(nonNegativeInts(max = 5).generateOne.value)(activityEntities(planEntities())): _*
      )
      .withDatasets(
        List.fill(nonNegativeInts(max = 5).generateOne.value)(datasetEntities(provenanceNonModified)): _*
      )

  def projectEntities(
      visibilityGen:       Gen[Visibility],
      minDateCreated:      projects.DateCreated = projects.DateCreated(Instant.EPOCH),
      activitiesFactories: List[ActivityGenFactory] = Nil,
      datasetsFactories:   List[DatasetGenFactory[Dataset.Provenance]] = Nil,
      forksCountGen:       Gen[ForksCount] = anyForksCount
  ): Gen[ProjectWithoutParent] = for {
    path         <- projectPaths
    name         <- Gen.const(path.toName)
    description  <- projectDescriptions
    agent        <- cliVersions
    dateCreated  <- projectCreatedDates(minDateCreated.value)
    maybeCreator <- personEntities(withGitLabId).toGeneratorOfOptions
    visibility   <- visibilityGen
    members      <- personEntities(withGitLabId).toGeneratorOfSet(minElements = 0)
    version      <- projectSchemaVersions
    forksCount   <- forksCountGen
    activities   <- activitiesFactories.map(_.apply(dateCreated)).sequence
    datasets     <- datasetsFactories.map(_.apply(dateCreated)).sequence
  } yield ProjectWithoutParent(path,
                               name,
                               description,
                               agent,
                               dateCreated,
                               maybeCreator,
                               visibility,
                               forksCount,
                               members ++ maybeCreator,
                               version,
                               activities,
                               datasets
  )

  def projectWithParentEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH)
  ): Gen[ProjectWithParent] = projectEntities(visibilityGen, minDateCreated).map(_.forkOnce()._2)

  implicit val forksCountZero:    Gen[ForksCount.Zero]    = Gen.const(ForksCount.Zero)
  implicit val forksCountNonZero: Gen[ForksCount.NonZero] = positiveInts(max = 100) map ForksCount.apply
  val anyForksCount:              Gen[ForksCount]         = Gen.oneOf(forksCountZero, forksCountNonZero)
  def fixedForksCount(count: Int Refined Positive): Gen[ForksCount.NonZero] = ForksCount(count)

  implicit lazy val gitLabProjectInfos: Gen[GitLabProjectInfo] = for {
    name            <- projectNames
    path            <- projectPaths
    description     <- projectDescriptions
    dateCreated     <- projectCreatedDates()
    maybeCreator    <- projectMemberObjects.toGeneratorOfOptions
    members         <- projectMemberObjects.toGeneratorOfSet()
    visibility      <- projectVisibilities
    maybeParentPath <- projectPaths.toGeneratorOfOptions
  } yield GitLabProjectInfo(name, path, dateCreated, description, maybeCreator, members, visibility, maybeParentPath)

  implicit lazy val projectMemberObjects: Gen[ProjectMember] = for {
    name     <- userNames
    username <- usernames
    gitLabId <- userGitLabIds
  } yield ProjectMember(name, username, gitLabId)

  implicit class ProjectGenFactoryOps[FC <: ForksCount](projectGen: Gen[Project])(implicit renkuBaseUrl: RenkuBaseUrl) {

    def withDatasets[P <: Dataset.Provenance](factories: DatasetGenFactory[P]*): Gen[Project] = for {
      project  <- projectGen
      datasets <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project match {
      case p: ProjectWithParent    => p.addDatasets(datasets: _*)
      case p: ProjectWithoutParent => p.addDatasets(datasets: _*)
    }

    def withActivities(factories: ActivityGenFactory*): Gen[Project] = for {
      project    <- projectGen
      activities <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project match {
      case p: ProjectWithParent    => p.addActivities(activities: _*)
      case p: ProjectWithoutParent => p.addActivities(activities: _*)
    }

    def addActivity(factory: Project => Gen[Activity]): Gen[Project] = for {
      project  <- projectGen
      activity <- factory(project)
    } yield project match {
      case p: ProjectWithParent    => p.addActivities(activity)
      case p: ProjectWithoutParent => p.addActivities(activity)
    }

    def addDataset[P <: Dataset.Provenance](factory: DatasetGenFactory[P]): Gen[(Dataset[P], Project)] = for {
      project <- projectGen
      ds      <- factory(project.dateCreated)
    } yield ds -> (project addDatasets ds)

    def addDatasetAndModification[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[((Dataset[P], Dataset[Dataset.Provenance.Modified]), Project)] = for {
      project    <- projectGen
      originalDs <- factory(project.dateCreated)
      modifiedDs <- originalDs.createModification()(project.dateCreated)
    } yield (originalDs -> modifiedDs) -> project.addDatasets(originalDs, modifiedDs)

    def addDatasetAndInvalidation[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[((Dataset[P], Dataset[Dataset.Provenance.Modified]), Project)] = for {
      project    <- projectGen
      originalDs <- factory(project.dateCreated)
      invalidated = originalDs.invalidateNow
    } yield (originalDs -> invalidated) -> project.addDatasets(originalDs, invalidated)

    def importDataset[PIN <: Dataset.Provenance, POUT <: Dataset.Provenance](
        dataset:              Dataset[PIN]
    )(implicit newProvenance: ProvenanceImportFactory[PIN, POUT]): Gen[(Dataset[POUT], Project)] =
      projectGen.map(_.importDataset(dataset))

    def modify(f: Project => Project): Gen[Project] = projectGen.map {
      case project: ProjectWithoutParent => f(project)
      case project: ProjectWithParent    => f(project)
    }

    def forkOnce(): Gen[(Project, ProjectWithParent)] = projectGen.map(_.forkOnce())
  }

  implicit class DatasetAndProjectOps[T](tupleGen: Gen[(T, Project)])(implicit renkuBaseUrl: RenkuBaseUrl) {

    def addDataset[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[((T, Dataset[P]), Project)] = for {
      tuple <- tupleGen
      (entities, project) = tuple
      ds <- factory(project.dateCreated)
    } yield (entities -> ds) -> (project addDatasets ds)

    def addDatasetAndModification[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[(((T, Dataset[P]), Dataset[Dataset.Provenance.Modified]), Project)] = for {
      tuple <- tupleGen
      (entities, project) = tuple
      originalDs <- factory(project.dateCreated)
      modifiedDs <- originalDs.createModification()(project.dateCreated)
    } yield ((entities -> originalDs) -> modifiedDs) -> project.addDatasets(originalDs, modifiedDs)

    def addDatasetAndInvalidation[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): Gen[(((T, Dataset[P]), Dataset[Dataset.Provenance.Modified]), Project)] = for {
      tuple <- tupleGen
      (entities, project) = tuple
      originalDs <- factory(project.dateCreated)
      invalidated = originalDs.invalidateNow
    } yield ((entities -> originalDs) -> invalidated) -> project.addDatasets(originalDs, invalidated)

    def importDataset[PIN <: Dataset.Provenance, POUT <: Dataset.Provenance](
        dataset:              Dataset[PIN]
    )(implicit newProvenance: ProvenanceImportFactory[PIN, POUT]): Gen[((T, Dataset[POUT]), Project)] =
      tupleGen map { case (entities, project) =>
        val (imported, updatedProject) = project.importDataset(dataset)
        ((entities, imported), updatedProject)
      }

    def forkOnce(): Gen[(T, (Project, ProjectWithParent))] =
      tupleGen map { case (entities, project) => entities -> project.forkOnce() }
  }

  lazy val membersLens: Lens[Project, Set[Person]] =
    Lens[Project, Set[Person]](_.members) { members =>
      {
        case project: ProjectWithoutParent => project.copy(members = members)
        case project: ProjectWithParent    => project.copy(members = members)
      }
    }

  lazy val creatorLens: Lens[Project, Option[Person]] =
    Lens[Project, Option[Person]](_.maybeCreator) { maybeCreator =>
      {
        case project: ProjectWithoutParent => project.copy(maybeCreator = maybeCreator)
        case project: ProjectWithParent    => project.copy(maybeCreator = maybeCreator)
      }
    }

  implicit class ProjectWithParentGenFactoryOps(projectGen: Gen[ProjectWithParent]) {

    def withDatasets[P <: Dataset.Provenance](factories: DatasetGenFactory[P]*): Gen[ProjectWithParent] = for {
      project  <- projectGen
      datasets <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addDatasets(datasets: _*)

    def withActivities(factories: ActivityGenFactory*): Gen[ProjectWithParent] = for {
      project    <- projectGen
      activities <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addActivities(activities: _*)

    def modify(f: ProjectWithParent => ProjectWithParent): Gen[ProjectWithParent] =
      projectGen.map(f)
  }

  implicit class ProjectWithoutParentGenFactoryOps(projectGen: Gen[ProjectWithoutParent]) {

    def withDatasets[P <: Dataset.Provenance](factories: DatasetGenFactory[P]*): Gen[ProjectWithoutParent] = for {
      project  <- projectGen
      datasets <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addDatasets(datasets: _*)

    def withActivities(factories: ActivityGenFactory*): Gen[ProjectWithoutParent] = for {
      project    <- projectGen
      activities <- factories.map(_.apply(project.dateCreated)).sequence
    } yield project.addActivities(activities: _*)

    def modify(f: ProjectWithoutParent => ProjectWithoutParent): Gen[ProjectWithoutParent] =
      projectGen.map(f)
  }
}
