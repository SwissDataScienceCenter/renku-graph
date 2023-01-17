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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{persons, projects}
import monocle.Lens
import org.scalacheck.Gen

trait ProjectEntitiesGenerators {
  self: EntitiesGenerators with RenkuProjectEntitiesGenerators with NonRenkuProjectEntitiesGenerators =>

  lazy val projectIdentifications: Gen[ProjectIdentification] =
    (projectResourceIds -> projectPaths).mapN(ProjectIdentification(_, _))

  lazy val anyProjectEntities: Gen[Project] = projectEntities(anyVisibility)

  def projectEntities(visibilityGen: Gen[Visibility]): Gen[Project] = Gen.oneOf(
    renkuProjectEntities(visibilityGen),
    renkuProjectWithParentEntities(visibilityGen),
    nonRenkuProjectEntities(visibilityGen),
    nonRenkuProjectWithParentEntities(visibilityGen)
  )

  lazy val memberGitLabIdLens: Lens[ProjectMember, persons.GitLabId] =
    Lens[ProjectMember, persons.GitLabId](_.gitLabId) { gitLabId =>
      {
        case member: ProjectMember.ProjectMemberNoEmail   => member.copy(gitLabId = gitLabId)
        case member: ProjectMember.ProjectMemberWithEmail => member.copy(gitLabId = gitLabId)
      }
    }

  def membersLens[P <: Project]: Lens[P, Set[Person]] =
    Lens[P, Set[Person]](_.members) { members =>
      _.fold(_.copy(members = members), _.copy(members = members), _.copy(members = members), _.copy(members = members))
        .asInstanceOf[P]
    }

  def creatorLens[P <: Project]: Lens[P, Option[Person]] =
    Lens[P, Option[Person]](_.maybeCreator) { maybeCreator =>
      _.fold(_.copy(maybeCreator = maybeCreator),
             _.copy(maybeCreator = maybeCreator),
             _.copy(maybeCreator = maybeCreator),
             _.copy(maybeCreator = maybeCreator)
      ).asInstanceOf[P]
    }

  def removeCreator[P <: Project](): P => P = creatorLens.modify(_ => None)

  def replaceProjectCreator[P <: Project](to: Option[Person]): P => P =
    _.fold(_.copy(maybeCreator = to), _.copy(maybeCreator = to), _.copy(maybeCreator = to), _.copy(maybeCreator = to))
      .asInstanceOf[P]

  def removeMembers[P <: Project](): P => P = membersLens.modify(_ => Set.empty)

  def replaceMembers[P <: Project](to: Set[Person]): P => P =
    _.fold(_.copy(members = to), _.copy(members = to), _.copy(members = to), _.copy(members = to)).asInstanceOf[P]

  def replaceProjectName[P <: Project](to: projects.Name): P => P =
    _.fold(_.copy(name = to), _.copy(name = to), _.copy(name = to), _.copy(name = to)).asInstanceOf[P]

  def replaceProjectKeywords[P <: Project](to: Set[projects.Keyword]): P => P =
    _.fold(_.copy(keywords = to), _.copy(keywords = to), _.copy(keywords = to), _.copy(keywords = to)).asInstanceOf[P]

  def replaceProjectDesc[P <: Project](to: Option[projects.Description]): P => P =
    _.fold(_.copy(maybeDescription = to),
           _.copy(maybeDescription = to),
           _.copy(maybeDescription = to),
           _.copy(maybeDescription = to)
    ).asInstanceOf[P]

  def replaceProjectDateCreated[P <: Project](to: projects.DateCreated): P => P =
    _.fold(_.copy(dateCreated = to), _.copy(dateCreated = to), _.copy(dateCreated = to), _.copy(dateCreated = to))
      .asInstanceOf[P]

  def replaceImages[P <: Project](to: List[ImageUri]): P => P =
    _.fold(_.copy(images = to), _.copy(images = to), _.copy(images = to), _.copy(images = to)).asInstanceOf[P]
}
