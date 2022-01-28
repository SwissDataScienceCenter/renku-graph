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

import io.renku.graph.model.projects.{DateCreated, Description, ForksCount, Keyword, Name, Path, Visibility}
import io.renku.graph.model.testentities.NonRenkuProject._
import io.renku.graph.model.testentities.RenkuProject._
import io.renku.graph.model.{GitLabApiUrl, RenkuBaseUrl, entities}
import io.renku.jsonld.{EntityIdEncoder, JsonLDEncoder}

trait Project extends Product with Serializable {
  val path:             Path
  val name:             Name
  val maybeDescription: Option[Description]
  val dateCreated:      DateCreated
  val maybeCreator:     Option[Person]
  val visibility:       Visibility
  val forksCount:       ForksCount
  val keywords:         Set[Keyword]
  val members:          Set[Person]

  type ProjectType <: Project
}

trait Parent {
  self: Project =>
  val parent: Project
}

object Project {

  import io.renku.jsonld.syntax._

  implicit def toEntitiesProject(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): Project => entities.Project = {
    case p: RenkuProject.WithParent       => toEntitiesRenkuProject(renkuBaseUrl)(p)
    case p: RenkuProject.WithoutParent    => toEntitiesRenkuProjectWithoutParent(renkuBaseUrl)(p)
    case p: NonRenkuProject.WithParent    => toEntitiesNonRenkuProjectWithParent(renkuBaseUrl)(p)
    case p: NonRenkuProject.WithoutParent => toEntitiesNonRenkuProjectWithoutParent(renkuBaseUrl)(p)
  }

  implicit def encoder[P <: Project](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: RenkuProject    => project.to[entities.RenkuProject].asJsonLD
    case project: NonRenkuProject => project.to[entities.NonRenkuProject].asJsonLD
  }

  implicit def entityIdEncoder[P <: Project](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[P] =
    EntityIdEncoder.instance(project => renkuBaseUrl / "projects" / project.path)
}
