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

package io.renku.graph.model
package testentities

import io.renku.cli.model.CliProject
import io.renku.graph.model.cli.CliConverters
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.{DateCreated, DateModified, Description, ForksCount, Keyword, Name, Path, Visibility}
import io.renku.graph.model.testentities.NonRenkuProject._
import io.renku.graph.model.testentities.RenkuProject._
import io.renku.graph.model.{GitLabApiUrl, GraphClass, RenkuUrl, entities}
import io.renku.jsonld.{EntityId, EntityIdEncoder, JsonLDEncoder}

trait Project extends Product with Serializable {
  val path:             Path
  val name:             Name
  val maybeDescription: Option[Description]
  val dateCreated:      DateCreated
  val dateModified:     DateModified
  val maybeCreator:     Option[Person]
  val visibility:       Visibility
  val forksCount:       ForksCount
  val keywords:         Set[Keyword]
  val members:          Set[Person]
  val images:           List[ImageUri]

  type ProjectType <: Project

  def fold[A](
      f1: RenkuProject.WithParent => A,
      f2: RenkuProject.WithoutParent => A,
      f3: NonRenkuProject.WithParent => A,
      f4: NonRenkuProject.WithoutParent => A
  ): A

  def identification(implicit renkuUrl: RenkuUrl): entities.ProjectIdentification =
    this.to[entities.ProjectIdentification]
}

trait Parent {
  self: Project =>
  val parent: Project
}

object Project {

  import cats.syntax.all._
  import io.renku.jsonld.syntax._

  implicit def functions[P <: Project](implicit renkuUrl: RenkuUrl): EntityFunctions[P] =
    EntityFunctions[entities.Project].contramap(implicitly[P => entities.Project])

  implicit def toEntitiesProject(implicit renkuUrl: RenkuUrl): Project => entities.Project =
    _.fold(
      toEntitiesRenkuProjectWithParent(renkuUrl),
      toEntitiesRenkuProjectWithoutParent(renkuUrl),
      toEntitiesNonRenkuProjectWithParent(renkuUrl),
      toEntitiesNonRenkuProjectWithoutParent(renkuUrl)
    )

  implicit def toCliProject(implicit renkuUrl: RenkuUrl): Project => CliProject =
    CliConverters.from(_)

  implicit def toProjectIdentification(implicit renkuUrl: RenkuUrl): Project => entities.ProjectIdentification =
    project => entities.ProjectIdentification(projects.ResourceId(project.asEntityId), project.path)

  implicit def encoder[P <: Project](implicit
      renkuUrl:     RenkuUrl,
      gitLabApiUrl: GitLabApiUrl,
      graph:        GraphClass
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: RenkuProject    => project.to[entities.RenkuProject].asJsonLD
    case project: NonRenkuProject => project.to[entities.NonRenkuProject].asJsonLD
  }

  implicit def entityIdEncoder[P <: Project](implicit renkuUrl: RenkuUrl): EntityIdEncoder[P] =
    EntityIdEncoder.instance(project => toEntityId(project.path))

  def toEntityId(projectPath: Path)(implicit renkuUrl: RenkuUrl): EntityId =
    EntityId.of(renkuUrl / "projects" / projectPath)
}
