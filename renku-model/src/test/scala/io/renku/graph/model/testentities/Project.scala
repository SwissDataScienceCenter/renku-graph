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

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model._
import io.renku.graph.model.projects.{DateCreated, Description, ForksCount, Name, Path, Visibility}
import io.renku.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory

sealed trait Project extends Project.ProjectOps with Product with Serializable {
  val path:         Path
  val name:         Name
  val description:  Description
  val agent:        CliVersion
  val dateCreated:  DateCreated
  val maybeCreator: Option[Person]
  val visibility:   Visibility
  val forksCount:   ForksCount
  val members:      Set[Person]
  val version:      SchemaVersion
  val activities:   List[Activity]
  val datasets:     List[Dataset[Dataset.Provenance]]

  type ProjectType <: Project

  lazy val plans: Set[Plan] = activities.map(_.association.plan).toSet
}

final case class ProjectWithoutParent(path:         Path,
                                      name:         Name,
                                      description:  Description,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      forksCount:   ForksCount,
                                      members:      Set[Person],
                                      version:      SchemaVersion,
                                      activities:   List[Activity],
                                      datasets:     List[Dataset[Dataset.Provenance]]
) extends Project {

  validateDates(dateCreated, activities, datasets)
    .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  override type ProjectType = ProjectWithoutParent

  override def addActivities(toAdd: Activity*): ProjectWithoutParent =
    copy(activities = activities ::: toAdd.toList)

  override def addDatasets[P <: Dataset.Provenance](toAdd: Dataset[P]*): ProjectWithoutParent =
    copy(datasets = datasets ::: toAdd.toList)

  override def addDataset[P <: Dataset.Provenance](
      factory: DatasetGenFactory[P]
  ): (Dataset[P], ProjectWithoutParent) = {
    val dataset = factory(dateCreated).generateOne
    dataset -> copy(datasets = datasets ::: dataset :: Nil)
  }

  private def validateDates(projectDateCreated: projects.DateCreated,
                            activities:         List[Activity],
                            datasets:           List[Dataset[Dataset.Provenance]]
  ): ValidatedNel[String, Unit] = activities
    .map { activity =>
      import activity._
      if ((startTime.value compareTo projectDateCreated.value) >= 0) ().validNel[String]
      else s"Activity $id startTime $startTime is older than project $projectDateCreated".invalidNel
    }
    .sequence
    .void |+| datasets
    .map {
      def compareDateWithProject(dataset: Dataset[Dataset.Provenance], dateCreated: model.datasets.DateCreated) =
        if ((dateCreated compareTo projectDateCreated.value) >= 0) ().validNel[String]
        else
          s"Dataset ${dataset.identification.identifier} startTime $dateCreated is older than project $projectDateCreated".invalidNel

      dataset =>
        dataset.provenance match {
          case p: Dataset.Provenance.Internal => compareDateWithProject(dataset, p.date)
          case p: Dataset.Provenance.Modified => compareDateWithProject(dataset, p.date)
          case _ => ().validNel[String]
        }
    }
    .sequence
    .void
}

final case class ProjectWithParent(path:         Path,
                                   name:         Name,
                                   description:  Description,
                                   agent:        CliVersion,
                                   dateCreated:  DateCreated,
                                   maybeCreator: Option[Person],
                                   visibility:   Visibility,
                                   forksCount:   ForksCount,
                                   members:      Set[Person],
                                   version:      SchemaVersion,
                                   activities:   List[Activity],
                                   datasets:     List[Dataset[Dataset.Provenance]],
                                   parent:       Project
) extends Project {
  override type ProjectType = ProjectWithParent

  override def addActivities(toAdd: Activity*): ProjectWithParent =
    copy(activities = activities ::: toAdd.toList)

  override def addDatasets[P <: Dataset.Provenance](toAdd: Dataset[P]*): ProjectWithParent =
    copy(datasets = datasets ::: toAdd.toList)

  override def addDataset[P <: Dataset.Provenance](factory: DatasetGenFactory[P]): (Dataset[P], ProjectWithParent) = {
    val dataset = factory(dateCreated).generateOne
    dataset -> copy(datasets = datasets ::: dataset :: Nil)
  }
}

object Project {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  trait ProjectOps {
    self: Project =>

    lazy val topAncestorDateCreated: DateCreated = this match {
      case project: ProjectWithParent => project.parent.topAncestorDateCreated
      case project => project.dateCreated
    }

    def addActivities(toAdd: Activity*): ProjectType

    def addDatasets[P <: Dataset.Provenance](toAdd: Dataset[P]*): ProjectType

    def addDataset[P <: Dataset.Provenance](toAdd: DatasetGenFactory[P]): (Dataset[P], ProjectType)
  }

  implicit def toEntitiesProject(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): Project => entities.Project = {
    case p: ProjectWithParent    => toEntitiesProjectWithParent(renkuBaseUrl)(p)
    case p: ProjectWithoutParent => toEntitiesProjectWithoutParent(renkuBaseUrl)(p)
  }

  implicit def toEntitiesProjectWithoutParent(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): ProjectWithoutParent => entities.ProjectWithoutParent =
    project =>
      entities.ProjectWithoutParent
        .from(
          projects.ResourceId(project.asEntityId),
          project.path,
          project.name,
          project.description,
          project.agent,
          project.dateCreated,
          project.maybeCreator.map(_.to[entities.Person]),
          project.visibility,
          project.members.map(_.to[entities.Person]),
          project.version,
          project.activities.map(_.to[entities.Activity]),
          project.datasets.map(_.to[entities.Dataset[entities.Dataset.Provenance]])
        )
        .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  implicit def toEntitiesProjectWithParent(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): ProjectWithParent => entities.ProjectWithParent =
    project =>
      entities.ProjectWithParent
        .from(
          projects.ResourceId(project.asEntityId),
          project.path,
          project.name,
          project.description,
          project.agent,
          project.dateCreated,
          project.maybeCreator.map(_.to[entities.Person]),
          project.visibility,
          project.members.map(_.to[entities.Person]),
          project.version,
          project.activities.map(_.to[entities.Activity]),
          project.datasets.map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          projects.ResourceId(project.parent.asEntityId)
        )
        .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  implicit def encoder[P <: Project](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: ProjectWithParent    => project.to[entities.ProjectWithParent].asJsonLD
    case project: ProjectWithoutParent => project.to[entities.ProjectWithoutParent].asJsonLD
  }

  implicit def entityIdEncoder[P <: Project](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[P] =
    EntityIdEncoder.instance(project => renkuBaseUrl / "projects" / project.path)
}
