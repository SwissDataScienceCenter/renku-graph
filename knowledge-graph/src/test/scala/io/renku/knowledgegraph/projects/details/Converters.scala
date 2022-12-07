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

package io.renku.knowledgegraph.projects.details

import KGProjectFinder._
import cats.syntax.all._
import io.renku.graph.model.testentities._

private object Converters extends Converters

private trait Converters {

  lazy val kgProjectConverter: Project => KGProject = {
    case project: RenkuProject.WithParent =>
      KGProject(
        project.resourceId,
        project.path,
        project.name,
        ProjectCreation(
          project.dateCreated,
          project.maybeCreator.map(_.to(projectCreatorConverter))
        ),
        project.visibility,
        project.parent.to(kgParentConverter).some,
        project.version.some,
        project.maybeDescription,
        project.keywords
      )
    case project: RenkuProject.WithoutParent =>
      KGProject(
        project.resourceId,
        project.path,
        project.name,
        ProjectCreation(
          project.dateCreated,
          project.maybeCreator.map(_.to(projectCreatorConverter))
        ),
        project.visibility,
        maybeParent = None,
        project.version.some,
        project.maybeDescription,
        project.keywords
      )
    case project: NonRenkuProject.WithParent =>
      KGProject(
        project.resourceId,
        project.path,
        project.name,
        ProjectCreation(
          project.dateCreated,
          project.maybeCreator.map(_.to(projectCreatorConverter))
        ),
        project.visibility,
        project.parent.to(kgParentConverter).some,
        maybeVersion = None,
        project.maybeDescription,
        project.keywords
      )
    case project: NonRenkuProject.WithoutParent =>
      KGProject(
        project.resourceId,
        project.path,
        project.name,
        ProjectCreation(
          project.dateCreated,
          project.maybeCreator.map(_.to(projectCreatorConverter))
        ),
        project.visibility,
        maybeParent = None,
        maybeVersion = None,
        project.maybeDescription,
        project.keywords
      )
    case other => throw new IllegalArgumentException(s"Project of unsupported type $other")
  }

  private lazy val kgParentConverter: Project => KGParent = parent =>
    KGParent(
      parent.resourceId,
      parent.path,
      parent.name,
      ProjectCreation(
        parent.dateCreated,
        parent.maybeCreator.map(_.to(projectCreatorConverter))
      )
    )

  private lazy val projectCreatorConverter: Person => ProjectCreator = person =>
    ProjectCreator(person.resourceId, person.name, person.maybeEmail, person.maybeAffiliation)

  lazy val toModelCreator: ProjectCreator => model.Creator = creator =>
    model.Creator(creator.resourceId, creator.name, creator.maybeEmail, creator.maybeAffiliation)
}
