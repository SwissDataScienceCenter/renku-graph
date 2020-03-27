/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import ch.datascience.generators.CommonGraphGenerators.{fusekiBaseUrls, renkuBaseUrls}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.{Person, Project}
import org.scalacheck.Gen

package object forks {

  implicit val renkuBaseUrl:  RenkuBaseUrl  = renkuBaseUrls.generateOne
  implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

  def gitLabProjects(parentPath: Path): Gen[GitLabProject] = gitLabProjects(
    maybeParentPaths = Gen.const(parentPath).toGeneratorOfSomes
  )

  def gitLabProjects(
      maybeParentPaths: Gen[Option[Path]] = projectPaths.toGeneratorOfOptions
  ): Gen[GitLabProject] =
    for {
      path            <- projectPaths
      maybeParentPath <- maybeParentPaths
      maybeCreator    <- gitLabCreator().toGeneratorOfOptions
      dateCreated     <- projectCreatedDates
    } yield GitLabProject(path, maybeParentPath, maybeCreator, dateCreated)

  def kgProjects(parentPath: Path): Gen[KGProject] = kgProjects(
    maybeParentResourceIds = Gen.const(ResourceId(renkuBaseUrl, parentPath)).toGeneratorOfSomes
  )

  def gitLabCreator(maybeEmail: Option[Email]      = userEmails.generateOption,
                    maybeName:  Option[users.Name] = userNames.generateOption): Gen[GitLabCreator] =
    GitLabCreator(maybeEmail, maybeName)

  def kgProjects(
      maybeParentResourceIds: Gen[Option[ResourceId]] = projectResourceIds.toGeneratorOfOptions
  ): Gen[KGProject] =
    for {
      resourceId            <- projectResourceIds
      maybeParentResourceId <- maybeParentResourceIds
      maybeCreator          <- kgCreator().toGeneratorOfOptions
      dateCreated           <- projectCreatedDates
    } yield KGProject(resourceId, maybeParentResourceId, maybeCreator, dateCreated)

  def kgCreator(maybeEmail: Option[Email] = userEmails.generateOption,
                name:       users.Name    = userNames.generateOne): Gen[KGCreator] =
    for {
      resourceId <- userResourceIds(maybeEmail)
    } yield KGCreator(resourceId, maybeEmail, name)

  implicit val entitiesProjects: Gen[Project] = entitiesProjects(
    maybeCreator       = entitiesPersons(userEmails.generateSome).generateOption,
    maybeParentProject = entitiesProjects(entitiesPersons(userEmails.generateSome).generateOption).generateOption
  )
  def entitiesProjects(maybeCreator:       Option[Person]  = entitiesPersons().generateOption,
                       maybeParentProject: Option[Project] = None): Gen[Project] =
    for {
      path        <- projectPaths
      name        <- projectNames
      createdDate <- projectCreatedDates
    } yield Project(path, name, createdDate, maybeCreator, maybeParentProject)

  def entitiesPersons(maybeEmailGen: Gen[Option[Email]] = userEmails.toGeneratorOfOptions): Gen[Person] =
    for {
      name       <- userNames
      maybeEmail <- maybeEmailGen
    } yield Person(name, maybeEmail)
}
