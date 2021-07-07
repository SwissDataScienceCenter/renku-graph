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

import cats.syntax.all._
import cats.data.ValidatedNel
import ch.datascience.graph.model.entities.Dataset.Provenance
import ch.datascience.graph.model.entities._
import ch.datascience.graph.model

private case class ProjectMetadata(project: Project, activities: List[Activity], datasets: List[Dataset[Provenance]])

private object ProjectMetadata {

  def from(project:    Project,
           activities: List[Activity],
           datasets:   List[Dataset[Provenance]]
  ): ValidatedNel[String, ProjectMetadata] = List(
    validateProjectRefs(project, activities, datasets),
    validateDates(project, activities, datasets)
  ).sequence.void.map(_ => ProjectMetadata(project, activities, datasets))

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
}
