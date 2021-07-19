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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.data.ValidatedNel
import cats.syntax.all._
import ch.datascience.graph.model
import ch.datascience.graph.model.entities.Dataset.Provenance
import ch.datascience.graph.model.entities.Dataset.Provenance.{ImportedInternal, Modified}
import ch.datascience.graph.model.entities._
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import monocle.{Lens, Traversal}

private trait ProjectMetadata extends ProjectMetadataOps {
  val project:    Project
  val activities: List[Activity]
  def datasets: List[Dataset[Provenance]]
}

private case class ProjectMetadataImpl(project:    Project,
                                       activities: List[Activity],
                                       datasets:   List[Dataset[Provenance]]
) extends ProjectMetadata

private object ProjectMetadata {

  import Lenses._

  def from(project:    Project,
           activities: List[Activity],
           datasets:   List[Dataset[Provenance]]
  ): ValidatedNel[String, ProjectMetadata] = List(
    validateProjectRefs(project, activities, datasets),
    validateDates(project, activities, datasets)
  ).sequence.void.map { _ =>
    (ProjectMetadataImpl.apply _).tupled(alignPersons(project, activities, datasets))
  }

  private def validateProjectRefs(project:    Project,
                                  activities: List[Activity],
                                  datasets:   List[Dataset[Provenance]]
  ): ValidatedNel[String, Unit] = activities
    .map(activity =>
      if (activity.projectResourceId == project.resourceId) ().validNel[String]
      else s"Activity ${activity.resourceId} points to a wrong project ${activity.projectResourceId}".invalidNel
    )
    .sequence
    .void |+| datasets
    .map(dataset =>
      if (dataset.projectResourceId == project.resourceId) ().validNel[String]
      else s"Dataset ${dataset.resourceId} points to a wrong project ${dataset.projectResourceId}".invalidNel
    )
    .sequence
    .void

  private def validateDates(project:    Project,
                            activities: List[Activity],
                            datasets:   List[Dataset[Provenance]]
  ): ValidatedNel[String, Unit] = project match {
    case _: ProjectWithParent => ().validNel[String]
    case _ =>
      activities
        .map { activity =>
          import activity._
          if ((startTime.value compareTo project.dateCreated.value) >= 0) ().validNel[String]
          else s"Activity $resourceId startTime $startTime is older than project ${project.dateCreated}".invalidNel
        }
        .sequence
        .void |+| datasets
        .map {
          def compareDateWithProject(dataset: Dataset[Provenance], dateCreated: model.datasets.DateCreated) =
            if ((dateCreated compareTo project.dateCreated.value) >= 0) ().validNel[String]
            else
              s"Dataset ${dataset.identification.identifier} startTime $dateCreated is older than project ${project.dateCreated}".invalidNel

          dataset =>
            dataset.provenance match {
              case p: Provenance.Internal => compareDateWithProject(dataset, p.date)
              case p: Provenance.Modified => compareDateWithProject(dataset, p.date)
              case _ => ().validNel[String]
            }
        }
        .sequence
        .void
  }

  private def alignPersons(project:    Project,
                           activities: List[Activity],
                           datasets:   List[Dataset[Provenance]]
  ): (Project, List[Activity], List[Dataset[Provenance]]) = {
    val projectPersons = collectPersons(project)
    (project, activities.updateAuthors(from = projectPersons), datasets.updateCreators(from = projectPersons))
  }

  private implicit class ActivitiesOps(activities: List[Activity]) {

    def updateAuthors(from: Set[Person]): List[Activity] =
      activitiesLens.modify { activity =>
        from
          .find(_.resourceId == activity.author.resourceId)
          .map(person => activityAuthorLens.modify(_ => person)(activity))
          .getOrElse(activity)
      }(activities)
  }

  private implicit class DatasetsOps(datasets: List[Dataset[Provenance]]) {

    def updateCreators(from: Set[Person]): List[Dataset[Provenance]] = {
      val creatorsUpdate = creatorsLens.modify { person =>
        from.find(_.resourceId == person.resourceId).getOrElse(person)
      }
      datasetsLens.modify(
        provenanceLens.modify(provCreatorsLens.modify(creatorsUpdate))
      )(datasets)
    }
  }

  private def collectPersons(project: Project): Set[Person] = project.members ++ project.maybeCreator

  implicit lazy val encoder: JsonLDEncoder[ProjectMetadata] = JsonLDEncoder.instance(_ => JsonLD.Null)

  object Lenses {
    val membersLens = Traversal.fromTraverse[List, Person]
    val projectMembersLens = Lens[Project, List[Person]](_.members.toList)(persons => {
      case p: ProjectWithoutParent => p.copy(members = persons.toSet)
      case p: ProjectWithParent    => p.copy(members = persons.toSet)
    })
    val projectCreatorLens = Lens[Project, Option[Person]](_.maybeCreator)(maybeCreator => {
      case p: ProjectWithoutParent => p.copy(maybeCreator = maybeCreator)
      case p: ProjectWithParent    => p.copy(maybeCreator = maybeCreator)
    })

    val activitiesLens     = Traversal.fromTraverse[List, Activity]
    val activityAuthorLens = Lens[Activity, Person](_.author)(p => a => a.copy(author = p))

    val datasetsLens   = Traversal.fromTraverse[List, Dataset[Provenance]]
    val provenanceLens = Lens[Dataset[Provenance], Provenance](_.provenance)(p => d => d.copy(provenance = p))
    val creatorsLens   = Traversal.fromTraverse[List, Person]
    val provCreatorsLens = Lens[Provenance, List[Person]](_.creators.toList) { crts =>
      {
        case p: Provenance.Internal                         => p.copy(creators = crts.toSet)
        case p: Provenance.ImportedExternal                 => p.copy(creators = crts.toSet)
        case p: Provenance.ImportedInternalAncestorExternal => p.copy(creators = crts.toSet)
        case p: Provenance.ImportedInternalAncestorInternal => p.copy(creators = crts.toSet)
        case p: Provenance.Modified                         => p.copy(creators = crts.toSet)
      }
    }
  }
}

private sealed trait ProjectMetadataOps {
  self: ProjectMetadata =>

  import ProjectMetadata.Lenses._

  def findAllPersons: Set[Person] =
    project.members ++ project.maybeCreator ++ activities.map(_.author) ++ datasets.flatMap(_.provenance.creators)

  def update(oldPerson: Person, newPerson: Person): ProjectMetadata = ProjectMetadataImpl(
    project = project.updateMember(oldPerson, newPerson).updateCreator(oldPerson, newPerson),
    activities = activities.updateAuthors(oldPerson, newPerson),
    datasets = datasets.updateCreators(oldPerson, newPerson)
  )

  def update(oldDataset: Dataset[Provenance], newDataset: Dataset[Provenance]): ProjectMetadata =
    if (oldDataset == newDataset) this
    else
      ProjectMetadataImpl(
        project = project,
        activities = activities,
        datasets = datasetsLens.modify {
          case dataset if dataset.resourceId == oldDataset.resourceId => newDataset
          case dataset                                                => dataset
        }(datasets)
      )

  def findInternallyImportedDatasets: List[Dataset[Provenance.ImportedInternal]] = datasets.flatMap { dataset =>
    dataset.provenance match {
      case _: ImportedInternal if dataset.maybeInvalidationTime.isEmpty =>
        Some(dataset.asInstanceOf[Dataset[Provenance.ImportedInternal]])
      case _ => Option.empty[Dataset[Dataset.Provenance.ImportedInternal]]
    }
  }

  def findModifiedDatasets: List[Dataset[Provenance.Modified]] = datasets.flatMap { dataset =>
    dataset.provenance match {
      case _: Modified if dataset.maybeInvalidationTime.isEmpty =>
        Some(dataset.asInstanceOf[Dataset[Provenance.Modified]])
      case _ => Option.empty[Dataset[Dataset.Provenance.Modified]]
    }
  }

  def findInvalidatedDatasets: List[Dataset[Provenance]] = datasets.filter(_.maybeInvalidationTime.isDefined)

  private implicit class ProjectOps(project: Project) {

    def updateMember(oldPerson: Person, newPerson: Person): Project =
      projectMembersLens.modify {
        membersLens.modify {
          case `oldPerson` => newPerson
          case p           => p
        }
      }(project)

    def updateCreator(oldPerson: Person, newPerson: Person): Project =
      projectCreatorLens.modify {
        case Some(`oldPerson`) => Some(newPerson)
        case other             => other
      }(project)
  }

  private implicit class ActivitiesOps(activities: List[Activity]) {

    def updateAuthors(oldPerson: Person, newPerson: Person): List[Activity] =
      activitiesLens.modify {
        activityAuthorLens.modify {
          case `oldPerson` => newPerson
          case p           => p
        }
      }(activities)
  }

  private implicit class DatasetsOps(datasets: List[Dataset[Provenance]]) {

    def updateCreators(oldPerson: Person, newPerson: Person): List[Dataset[Provenance]] =
      datasetsLens.modify(
        provenanceLens.modify(
          provCreatorsLens.modify(
            creatorsLens.modify {
              case `oldPerson` => newPerson
              case p           => p
            }
          )
        )
      )(datasets)
  }
}
