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

import ch.datascience.graph.model._
import ch.datascience.graph.model.projects.{DateCreated, ForksCount, Name, Path, Visibility}

sealed trait Project[+FC <: ForksCount] extends Project.ProjectOps[FC] {
  val path:         Path
  val name:         Name
  val agent:        CliVersion
  val dateCreated:  DateCreated
  val maybeCreator: Option[Person]
  val visibility:   Visibility
  val forksCount:   FC
  val members:      Set[Person]
  val version:      SchemaVersion
}

final case class ProjectWithoutParent[+FC <: ForksCount](path:         Path,
                                                         name:         Name,
                                                         agent:        CliVersion,
                                                         dateCreated:  DateCreated,
                                                         maybeCreator: Option[Person],
                                                         visibility:   Visibility,
                                                         forksCount:   FC,
                                                         members:      Set[Person],
                                                         version:      SchemaVersion
) extends Project[FC]

final case class ProjectWithParent[+FC <: ForksCount](path:         Path,
                                                      name:         Name,
                                                      agent:        CliVersion,
                                                      dateCreated:  DateCreated,
                                                      maybeCreator: Option[Person],
                                                      visibility:   Visibility,
                                                      forksCount:   FC,
                                                      members:      Set[Person],
                                                      version:      SchemaVersion,
                                                      parent:       Project[ForksCount.NonZero]
) extends Project[FC]

object Project {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def withoutParent[FC <: ForksCount](path:         Path,
                                      name:         Name,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      forksCount:   FC,
                                      members:      Set[Person],
                                      version:      SchemaVersion
  ): ProjectWithoutParent[FC] =
    ProjectWithoutParent[FC](path, name, agent, dateCreated, maybeCreator, visibility, forksCount, members, version)

  def withParent[FC <: ForksCount](path:          Path,
                                   name:          Name,
                                   agent:         CliVersion,
                                   dateCreated:   DateCreated,
                                   maybeCreator:  Option[Person],
                                   visibility:    Visibility,
                                   forksCount:    FC,
                                   members:       Set[Person],
                                   version:       SchemaVersion,
                                   parentProject: Project[ForksCount.NonZero]
  ): ProjectWithParent[FC] = new ProjectWithParent[FC](path,
                                                       name,
                                                       agent,
                                                       dateCreated,
                                                       maybeCreator,
                                                       visibility,
                                                       forksCount,
                                                       members,
                                                       version,
                                                       parentProject
  )

  trait ProjectOps[+FC <: ForksCount] {
    self: Project[FC] =>

    lazy val topAncestorDateCreated: DateCreated = this match {
      case project: ProjectWithParent[_] => project.parent.topAncestorDateCreated
      case project => project.dateCreated
    }
  }

  implicit def toEntitiesProject(implicit renkuBaseUrl: RenkuBaseUrl): Project[ForksCount] => entities.Project = {
    case p: ProjectWithParent[ForksCount]    => toEntitiesProjectWithParent(renkuBaseUrl)(p)
    case p: ProjectWithoutParent[ForksCount] => toEntitiesProjectWithoutParent(renkuBaseUrl)(p)
  }

  private def toEntitiesProjectWithoutParent(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): ProjectWithoutParent[ForksCount] => entities.ProjectWithoutParent =
    project =>
      entities.ProjectWithoutParent(
        projects.ResourceId(project.asEntityId),
        project.path,
        project.name,
        project.agent,
        project.dateCreated,
        project.maybeCreator.map(_.to[entities.Person]),
        project.visibility,
        project.members.map(_.to[entities.Person]),
        project.version
      )

  private def toEntitiesProjectWithParent(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): ProjectWithParent[ForksCount] => entities.ProjectWithParent =
    project =>
      entities.ProjectWithParent(
        projects.ResourceId(project.asEntityId),
        project.path,
        project.name,
        project.agent,
        project.dateCreated,
        project.maybeCreator.map(_.to[entities.Person]),
        project.visibility,
        project.members.map(_.to[entities.Person]),
        project.version,
        project.parent.resourceId
      )

  implicit def encoder[P <: Project[_]](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: ProjectWithParent[_] =>
      JsonLD.arr(
        project.to[entities.Project].asJsonLD,
        project.parent.to[entities.Project].asJsonLD
      )
    case project: Project[_] => project.to[entities.Project].asJsonLD
  }

  implicit def entityIdEncoder[P <: Project[_]](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[P] =
    EntityIdEncoder.instance(project => renkuBaseUrl / "projects" / project.path)
}
