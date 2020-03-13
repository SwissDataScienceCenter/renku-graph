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

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.users.Email
import org.scalacheck.Gen

package object forks {

  private val renkuBaseUrl = renkuBaseUrls.generateOne

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

  def gitLabCreator(maybeEmail: Option[Email] = emails.generateOption): Gen[GitLabCreator] =
    for {
      maybeName <- names.toGeneratorOfOptions
    } yield GitLabCreator(maybeEmail, maybeName)

  def kgProjects(
      maybeParentResourceIds: Gen[Option[ResourceId]] = projectResourceIds.toGeneratorOfOptions
  ): Gen[KGProject] =
    for {
      resourceId            <- projectResourceIds
      maybeParentResourceId <- maybeParentResourceIds
      creator               <- kgCreator()
      dateCreated           <- projectCreatedDates
    } yield KGProject(resourceId, maybeParentResourceId, creator, dateCreated)

  def kgCreator(maybeEmail: Option[Email] = emails.generateOption): Gen[KGCreator] =
    for {
      resourceId <- userResourceIds(maybeEmail)
      maybeName  <- names.toGeneratorOfOptions
    } yield KGCreator(resourceId, maybeEmail, maybeName)
}
