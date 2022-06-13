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

package io.renku.graph.model.testentities

import io.renku.graph.model._
import io.renku.graph.model.projects.{DateCreated, Description, ForksCount, Keyword, Name, Path, Visibility}
import io.renku.jsonld.JsonLDEncoder
import io.renku.jsonld.syntax._

sealed trait NonRenkuProject extends Project with Product with Serializable

object NonRenkuProject {

  final case class WithoutParent(path:             Path,
                                 name:             Name,
                                 maybeDescription: Option[Description],
                                 dateCreated:      DateCreated,
                                 maybeCreator:     Option[Person],
                                 visibility:       Visibility,
                                 forksCount:       ForksCount,
                                 keywords:         Set[Keyword],
                                 members:          Set[Person]
  ) extends NonRenkuProject {
    override type ProjectType = NonRenkuProject.WithoutParent
  }

  final case class WithParent(path:             Path,
                              name:             Name,
                              maybeDescription: Option[Description],
                              dateCreated:      DateCreated,
                              maybeCreator:     Option[Person],
                              visibility:       Visibility,
                              forksCount:       ForksCount,
                              keywords:         Set[Keyword],
                              members:          Set[Person],
                              parent:           NonRenkuProject
  ) extends NonRenkuProject
      with Parent {
    override type ProjectType = NonRenkuProject.WithParent
  }

  implicit def toEntitiesNonRenkuProject(implicit renkuUrl: RenkuUrl): NonRenkuProject => entities.NonRenkuProject = {
    case p: NonRenkuProject.WithParent    => toEntitiesNonRenkuProjectWithParent(renkuUrl)(p)
    case p: NonRenkuProject.WithoutParent => toEntitiesNonRenkuProjectWithoutParent(renkuUrl)(p)
  }

  implicit def toEntitiesNonRenkuProjectWithoutParent(implicit
      renkuUrl: RenkuUrl
  ): NonRenkuProject.WithoutParent => entities.NonRenkuProject.WithoutParent =
    project =>
      entities.NonRenkuProject.WithoutParent(
        projects.ResourceId(project.asEntityId),
        project.path,
        project.name,
        project.maybeDescription,
        project.dateCreated,
        project.maybeCreator.map(_.to[entities.Person]),
        project.visibility,
        project.keywords,
        project.members.map(_.to[entities.Person])
      )

  implicit def toEntitiesNonRenkuProjectWithParent(implicit
      renkuUrl: RenkuUrl
  ): NonRenkuProject.WithParent => entities.NonRenkuProject.WithParent =
    project =>
      entities.NonRenkuProject.WithParent(
        projects.ResourceId(project.asEntityId),
        project.path,
        project.name,
        project.maybeDescription,
        project.dateCreated,
        project.maybeCreator.map(_.to[entities.Person]),
        project.visibility,
        project.keywords,
        project.members.map(_.to[entities.Person]),
        projects.ResourceId(project.parent.asEntityId)
      )

  implicit def encoder[P <: NonRenkuProject](implicit
      renkuUrl:     RenkuUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: NonRenkuProject.WithParent    => project.to[entities.NonRenkuProject.WithParent].asJsonLD
    case project: NonRenkuProject.WithoutParent => project.to[entities.NonRenkuProject.WithoutParent].asJsonLD
  }
}
