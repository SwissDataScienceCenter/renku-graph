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
package generators

import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{persons, projects}
import monocle.Lens
import org.scalacheck.Gen

trait ProjectEntitiesGenerators {
  self: EntitiesGenerators with RenkuProjectEntitiesGenerators with NonRenkuProjectEntitiesGenerators =>

  lazy val anyProjectEntities: Gen[Project] = projectEntities(anyVisibility)

  def projectEntities(visibilityGen: Gen[Visibility]): Gen[Project] = Gen.oneOf(
    renkuProjectEntities(visibilityGen),
    renkuProjectWithParentEntities(visibilityGen),
    nonRenkuProjectEntities(visibilityGen),
    nonRenkuProjectWithParentEntities(visibilityGen)
  )

  implicit class ProjectGenFactoryOps(projectGen: Gen[Project]) {
    def modify(f: Project => Project): Gen[Project] = projectGen.map(f)
  }

  lazy val memberGitLabIdLens: Lens[ProjectMember, persons.GitLabId] =
    Lens[ProjectMember, persons.GitLabId](_.gitLabId) { gitLabId =>
      {
        case member: ProjectMember.ProjectMemberNoEmail   => member.copy(gitLabId = gitLabId)
        case member: ProjectMember.ProjectMemberWithEmail => member.copy(gitLabId = gitLabId)
      }
    }

  def membersLens[P <: Project]: Lens[P, Set[Person]] =
    Lens[P, Set[Person]](_.members) { members =>
      {
        case project: RenkuProject.WithoutParent    => project.copy(members = members).asInstanceOf[P]
        case project: RenkuProject.WithParent       => project.copy(members = members).asInstanceOf[P]
        case project: NonRenkuProject.WithoutParent => project.copy(members = members).asInstanceOf[P]
        case project: NonRenkuProject.WithParent    => project.copy(members = members).asInstanceOf[P]
      }
    }

  def creatorLens[P <: Project]: Lens[P, Option[Person]] =
    Lens[P, Option[Person]](_.maybeCreator) { maybeCreator =>
      {
        case project: RenkuProject.WithoutParent    => project.copy(maybeCreator = maybeCreator).asInstanceOf[P]
        case project: RenkuProject.WithParent       => project.copy(maybeCreator = maybeCreator).asInstanceOf[P]
        case project: NonRenkuProject.WithoutParent => project.copy(maybeCreator = maybeCreator).asInstanceOf[P]
        case project: NonRenkuProject.WithParent    => project.copy(maybeCreator = maybeCreator).asInstanceOf[P]
      }
    }

  def removeCreator[P <: Project](): P => P = creatorLens.modify(_ => None)

  def replaceProjectCreator[P <: Project](to: Option[Person]): P => P = {
    case project: RenkuProject.WithoutParent    => project.copy(maybeCreator = to).asInstanceOf[P]
    case project: RenkuProject.WithParent       => project.copy(maybeCreator = to).asInstanceOf[P]
    case project: NonRenkuProject.WithoutParent => project.copy(maybeCreator = to).asInstanceOf[P]
    case project: NonRenkuProject.WithParent    => project.copy(maybeCreator = to).asInstanceOf[P]
  }

  def removeMembers[P <: Project](): P => P = membersLens.modify(_ => Set.empty)

  def replaceMembers[P <: Project](to: Set[Person]): P => P = {
    case project: RenkuProject.WithoutParent    => project.copy(members = to).asInstanceOf[P]
    case project: RenkuProject.WithParent       => project.copy(members = to).asInstanceOf[P]
    case project: NonRenkuProject.WithoutParent => project.copy(members = to).asInstanceOf[P]
    case project: NonRenkuProject.WithParent    => project.copy(members = to).asInstanceOf[P]
  }

  def replaceProjectName[P <: Project](to: projects.Name): P => P = {
    case project: RenkuProject.WithoutParent    => project.copy(name = to).asInstanceOf[P]
    case project: RenkuProject.WithParent       => project.copy(name = to).asInstanceOf[P]
    case project: NonRenkuProject.WithoutParent => project.copy(name = to).asInstanceOf[P]
    case project: NonRenkuProject.WithParent    => project.copy(name = to).asInstanceOf[P]
  }

  def replaceProjectKeywords[P <: Project](to: Set[projects.Keyword]): P => P = {
    case project: RenkuProject.WithoutParent    => project.copy(keywords = to).asInstanceOf[P]
    case project: RenkuProject.WithParent       => project.copy(keywords = to).asInstanceOf[P]
    case project: NonRenkuProject.WithoutParent => project.copy(keywords = to).asInstanceOf[P]
    case project: NonRenkuProject.WithParent    => project.copy(keywords = to).asInstanceOf[P]
  }

  def replaceProjectDesc[P <: Project](to: Option[projects.Description]): P => P = {
    case project: RenkuProject.WithoutParent    => project.copy(maybeDescription = to).asInstanceOf[P]
    case project: RenkuProject.WithParent       => project.copy(maybeDescription = to).asInstanceOf[P]
    case project: NonRenkuProject.WithoutParent => project.copy(maybeDescription = to).asInstanceOf[P]
    case project: NonRenkuProject.WithParent    => project.copy(maybeDescription = to).asInstanceOf[P]
  }

  def replaceProjectDateCreated[P <: Project](to: projects.DateCreated): P => P = {
    case project: RenkuProject.WithoutParent    => project.copy(dateCreated = to).asInstanceOf[P]
    case project: RenkuProject.WithParent       => project.copy(dateCreated = to).asInstanceOf[P]
    case project: NonRenkuProject.WithoutParent => project.copy(dateCreated = to).asInstanceOf[P]
    case project: NonRenkuProject.WithParent    => project.copy(dateCreated = to).asInstanceOf[P]
  }
}
