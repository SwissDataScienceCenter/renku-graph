/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.gitlab.{GitLabMember, GitLabUser}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.versions.SchemaVersion
import io.renku.graph.model.{persons, projects}
import monocle.{Lens, Traversal}
import org.scalacheck.Gen

trait ProjectEntitiesGenerators {
  self: EntitiesGenerators with RenkuProjectEntitiesGenerators with NonRenkuProjectEntitiesGenerators =>

  lazy val projectIdentifications: Gen[ProjectIdentification] =
    (projectResourceIds -> projectSlugs).mapN(ProjectIdentification(_, _))

  lazy val anyProjectEntities: Gen[Project] = projectEntities(anyVisibility)

  def projectEntities(visibilityGen: Gen[Visibility],
                      creatorGen:    Gen[Person] = personEntities(withGitLabId)
  ): Gen[Project] = Gen.oneOf(
    renkuProjectEntities(visibilityGen, creatorGen = creatorGen),
    renkuProjectWithParentEntities(visibilityGen, creatorGen = creatorGen),
    nonRenkuProjectEntities(visibilityGen, creatorGen = creatorGen),
    nonRenkuProjectWithParentEntities(visibilityGen, creatorGen = creatorGen)
  )

  lazy val memberGitLabUserLens: Lens[GitLabMember, GitLabUser] =
    Lens[GitLabMember, GitLabUser](_.user)(a => b => b.copy(user = a))

  lazy val gitLabUserIdLens: Lens[GitLabUser, persons.GitLabId] =
    Lens[GitLabUser, persons.GitLabId](_.gitLabId)(a => b => b.copy(gitLabId = a))

  lazy val gitLabUserEmailLens: Lens[GitLabUser, Option[persons.Email]] =
    Lens[GitLabUser, Option[persons.Email]](_.email)(a => b => b.copy(email = a))

  lazy val memberGitLabIdLens: Lens[GitLabMember, persons.GitLabId] =
    memberGitLabUserLens.andThen(gitLabUserIdLens)

  lazy val memberGitLabEmailLens: Lens[GitLabMember, Option[persons.Email]] =
    memberGitLabUserLens.andThen(gitLabUserEmailLens)

  def membersLens[P <: Project]: Lens[P, Set[Project.Member]] =
    Lens[P, Set[Project.Member]](_.members) { members =>
      _.fold(_.copy(members = members), _.copy(members = members), _.copy(members = members), _.copy(members = members))
        .asInstanceOf[P]
    }

  def membersLensAsList[P <: Project]: Lens[P, List[Project.Member]] =
    Lens[P, List[Project.Member]](_.members.toList.sortBy(_.person.name.value)) { members =>
      _.fold(_.copy(members = members.toSet),
             _.copy(members = members.toSet),
             _.copy(members = members.toSet),
             _.copy(members = members.toSet)
      )
        .asInstanceOf[P]
    }

  def memberPersonLens: Lens[Project.Member, Person] =
    Lens[Project.Member, Person](_.person)(a => b => b.copy(person = a))

  def personNameLens: Lens[Person, persons.Name] =
    Lens[Person, persons.Name](_.name)(a => b => b.copy(name = a))

  def personGitLabIdLens: Lens[Person, Option[persons.GitLabId]] =
    Lens[Person, Option[persons.GitLabId]](_.maybeGitLabId)(a => b => b.copy(maybeGitLabId = a))

  def memberPersonNameLens: Lens[Project.Member, persons.Name] =
    memberPersonLens.andThen(personNameLens)

  def memberPersonGitLabIdLens: Lens[Project.Member, Option[persons.GitLabId]] =
    memberPersonLens.andThen(personGitLabIdLens)

  def projectMembersPersonLens[P <: Project]: Traversal[P, Person] = {
    val t = Traversal.fromTraverse[List, Project.Member]
    membersLensAsList.andThen(t).andThen(memberPersonLens)
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
    creatorLens[P].replace(to)

  def replaceVisibility[P <: Project](to: Visibility): P => P =
    _.fold(_.copy(visibility = to), _.copy(visibility = to), _.copy(visibility = to), _.copy(visibility = to))
      .asInstanceOf[P]

  def removeMembers[P <: Project](): P => P = membersLens.replace(Set.empty)

  def replaceMembers[P <: Project](to: Set[Project.Member]): P => P = membersLens.replace(to)

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

  def replaceProjectDateModified[P <: Project](to: projects.DateModified): P => P =
    _.fold(_.copy(dateModified = to), _.copy(dateModified = to), _.copy(dateModified = to), _.copy(dateModified = to))
      .asInstanceOf[P]

  def replaceSchemaVersion[P <: Project](to: SchemaVersion): P => P =
    _.fold(_.copy(version = to), _.copy(version = to), identity, identity).asInstanceOf[P]

  def replaceImages[P <: Project](to: List[ImageUri]): P => P =
    _.fold(_.copy(images = to), _.copy(images = to), _.copy(images = to), _.copy(images = to)).asInstanceOf[P]
}
