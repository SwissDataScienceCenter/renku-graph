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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration

import ch.datascience.generators.CommonGraphGenerators.{fusekiBaseUrls, gitLabUrls, renkuBaseUrls}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Name, Path, Visibility}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.EntitiesGenerators._
import ch.datascience.rdfstore.entities.{Person, Project}
import org.scalacheck.Gen

package object projects {

  implicit val renkuBaseUrl:  RenkuBaseUrl  = renkuBaseUrls.generateOne
  implicit val gitLabApiUrl:  GitLabApiUrl  = gitLabUrls.generateOne.apiV4
  implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

  private[projects] def gitLabProjects(projectPath: Path, parentPath: Path): Gen[GitLabProject] =
    gitLabProjects(projectPath = projectPath, maybeParentPaths = Gen.const(parentPath).toGeneratorOfSomes)

  private[projects] def gitLabProjects(
      projectPath:      Path,
      maybeParentPaths: Gen[Option[Path]] = projectPaths.toGeneratorOfOptions
  ): Gen[GitLabProject] =
    for {
      name            <- projectNames
      visibility      <- projectVisibilities
      maybeParentPath <- maybeParentPaths
      maybeCreator    <- gitLabCreator().toGeneratorOfOptions
      dateCreated     <- projectCreatedDates
    } yield GitLabProject(projectPath, name, visibility, dateCreated, maybeParentPath, maybeCreator)

  private[projects] def gitLabCreator(gitLabId: users.GitLabId = userGitLabIds.generateOne,
                                      name:     users.Name = userNames.generateOne
  ): Gen[GitLabCreator] = GitLabCreator(gitLabId, name)

  private[projects] def kgCreator(maybeEmail: Option[Email] = userEmails.generateOption,
                                  name:       users.Name = userNames.generateOne
  ): Gen[KGCreator] =
    for {
      resourceId <- userResourceIds(maybeEmail)
    } yield KGCreator(resourceId, maybeEmail, name)

  implicit val entitiesProjects: Gen[Project] = entitiesProjects(
    maybeCreator = persons.generateOption,
    maybeParentProject = entitiesProjects(maybeCreator = persons.generateOption).generateOption
  )

  def entitiesProjects(name:               Name = projectNames.generateOne,
                       maybeCreator:       Option[Person] = persons.generateOption,
                       maybeParentProject: Option[Project] = None,
                       maybeVisibility:    Option[Visibility] = None
  ): Gen[Project] = for {
    path        <- projectPaths
    createdDate <- projectCreatedDates
    version     <- projectSchemaVersions
  } yield Project(path,
                  name,
                  createdDate,
                  maybeCreator,
                  maybeVisibility,
                  maybeParentProject = maybeParentProject,
                  version = version
  )
}
