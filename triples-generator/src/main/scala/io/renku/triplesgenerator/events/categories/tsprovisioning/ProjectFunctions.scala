/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.tsprovisioning

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.entities.Dataset.Provenance.{ImportedInternal, Modified}
import io.renku.graph.model.entities._
import monocle.{Lens, Traversal}

import scala.annotation.tailrec

private trait ProjectFunctions {

  import ProjectFunctions.Lenses._
  import ProjectFunctions._

  lazy val findAllPersons: Project => Set[Person] = project =>
    project.members ++
      project.maybeCreator ++
      project.activities.map(_.author) ++
      project.datasets.flatMap(_.provenance.creators.toList.toSet) ++
      project.activities.flatMap(_.association.agent match {
        case p: Person => Option(p)
        case _ => Option.empty[Person]
      })

  def update(oldPerson: Person, newPerson: Person): Project => Project = project =>
    project
      .updateMember(oldPerson, newPerson)
      .updateCreator(oldPerson, newPerson)
      .updateActivities(_.updateAuthorsAndAgents(oldPerson, newPerson))
      .updateDatasets(_.updateCreators(oldPerson, newPerson))

  def update(oldDataset: Dataset[Provenance], newDataset: Dataset[Provenance]): Project => Project = project =>
    if (oldDataset == newDataset) project
    else
      project.updateDatasets(
        datasetsLens modify {
          case `oldDataset` => newDataset
          case dataset      => dataset
        }
      )

  lazy val findInternallyImportedDatasets: Project => List[Dataset[Provenance.ImportedInternal]] = project => {
    val invalidatedDatasets = findInvalidatedDatasets(project)

    project.datasets.flatMap { dataset =>
      dataset.provenance match {
        case _: ImportedInternal =>
          Option.unless(
            invalidatedDatasets.exists(_.identification.resourceId == dataset.identification.resourceId)
          )(dataset.asInstanceOf[Dataset[Provenance.ImportedInternal]])
        case _ => Option.empty[Dataset[Dataset.Provenance.ImportedInternal]]
      }
    }
  }

  lazy val findModifiedDatasets: Project => List[Dataset[Provenance.Modified]] = project => {
    val invalidatedDatasets = findInvalidatedDatasets(project)

    project.datasets flatMap { dataset =>
      dataset.provenance match {
        case p: Modified if p.maybeInvalidationTime.isEmpty =>
          Option.unless(
            invalidatedDatasets.exists(_.identification.resourceId == dataset.identification.resourceId)
          )(dataset.asInstanceOf[Dataset[Provenance.Modified]])
        case _ => Option.empty[Dataset[Dataset.Provenance.Modified]]
      }
    }
  }

  lazy val findInvalidatedDatasets: Project => List[Dataset[Provenance]] = project =>
    project.datasets.foldLeft(List.empty[Dataset[Provenance]]) { (invalidateDatasets, dataset) =>
      dataset.provenance match {
        case p: Modified if p.maybeInvalidationTime.isDefined =>
          project.datasets
            .find(_.resourceId.value == p.derivedFrom.value)
            .map(_ :: invalidateDatasets)
            .getOrElse(invalidateDatasets)
        case _ => invalidateDatasets
      }
    }

  def findTopmostDerivedFrom[F[_]: MonadThrow](dataset: Dataset[Provenance],
                                               project: Project
  ): F[model.datasets.TopmostDerivedFrom] = {
    import model.datasets.TopmostDerivedFrom

    @tailrec
    def findParent(topmostDerivedFrom: TopmostDerivedFrom): F[TopmostDerivedFrom] =
      project.datasets.find(_.identification.resourceId.show == topmostDerivedFrom.show) match {
        case None =>
          new IllegalStateException(show"Broken derivation hierarchy for $topmostDerivedFrom")
            .raiseError[F, TopmostDerivedFrom]
        case Some(ds) =>
          ds.provenance match {
            case prov: Provenance.Modified => findParent(prov.topmostDerivedFrom)
            case prov => prov.topmostDerivedFrom.pure[F]
          }
      }

    findParent(dataset.provenance.topmostDerivedFrom)
  }
}

private object ProjectFunctions extends ProjectFunctions {

  import Lenses._

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

    def updateActivities(function: List[Activity] => List[Activity]): Project =
      projectActivitiesLens.modify(function)(project)

    def updateDatasets(function: List[Dataset[Dataset.Provenance]] => List[Dataset[Dataset.Provenance]]): Project =
      projectDatasetsLens.modify(function)(project)
  }

  private implicit class ActivitiesOps(activities: List[Activity]) {

    def updateAuthorsAndAgents(oldPerson: Person, newPerson: Person): List[Activity] =
      activitiesLens.modify {
        updateAuthor(oldPerson, newPerson) >>> updateAssociationAgents(oldPerson, newPerson)
      }(activities)

    private def updateAuthor(oldPerson: Person, newPerson: Person) =
      activityAuthorLens.modify {
        case `oldPerson` => newPerson
        case p           => p
      }

    private def updateAssociationAgents(oldPerson: Person, newPerson: Person): Activity => Activity = { activity =>
      activity.association match {
        case _:     Association.WithRenkuAgent => activity
        case assoc: Association.WithPersonAgent =>
          assoc.agent match {
            case `oldPerson` => activity.copy(association = assoc.copy(agent = newPerson))
            case _           => activity
          }
      }
    }
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

  private object Lenses {
    val membersLens = Traversal.fromTraverse[List, Person]
    val projectMembersLens = Lens[Project, List[Person]](_.members.toList)(persons => {
      case p: RenkuProject.WithoutParent    => p.copy(members = persons.toSet)
      case p: RenkuProject.WithParent       => p.copy(members = persons.toSet)
      case p: NonRenkuProject.WithoutParent => p.copy(members = persons.toSet)
      case p: NonRenkuProject.WithParent    => p.copy(members = persons.toSet)
    })
    val projectCreatorLens = Lens[Project, Option[Person]](_.maybeCreator)(maybeCreator => {
      case p: RenkuProject.WithoutParent    => p.copy(maybeCreator = maybeCreator)
      case p: RenkuProject.WithParent       => p.copy(maybeCreator = maybeCreator)
      case p: NonRenkuProject.WithoutParent => p.copy(maybeCreator = maybeCreator)
      case p: NonRenkuProject.WithParent    => p.copy(maybeCreator = maybeCreator)
    })

    val projectActivitiesLens = Lens[Project, List[Activity]](_.activities)(activities => {
      case p: RenkuProject.WithoutParent => p.copy(activities = activities)
      case p: RenkuProject.WithParent    => p.copy(activities = activities)
      case p: NonRenkuProject            => p
    })
    val activitiesLens: Traversal[List[Activity], Activity] = Traversal.fromTraverse[List, Activity]
    val activityAuthorLens = Lens[Activity, Person](_.author)(p => a => a.copy(author = p))

    val projectDatasetsLens = Lens[Project, List[Dataset[Dataset.Provenance]]](_.datasets)(datasets => {
      case p: RenkuProject.WithoutParent => p.copy(datasets = datasets)
      case p: RenkuProject.WithParent    => p.copy(datasets = datasets)
      case p: NonRenkuProject            => p
    })
    val datasetsLens   = Traversal.fromTraverse[List, Dataset[Provenance]]
    val provenanceLens = Lens[Dataset[Provenance], Provenance](_.provenance)(p => d => d.copy(provenance = p))
    val creatorsLens   = Traversal.fromTraverse[NonEmptyList, Person]
    val provCreatorsLens = Lens[Provenance, NonEmptyList[Person]](_.creators) { crts =>
      {
        case p: Provenance.Internal                         => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.ImportedExternal                 => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.ImportedInternalAncestorExternal => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.ImportedInternalAncestorInternal => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.Modified                         => p.copy(creators = crts.sortBy(_.name))
      }
    }
  }
}
