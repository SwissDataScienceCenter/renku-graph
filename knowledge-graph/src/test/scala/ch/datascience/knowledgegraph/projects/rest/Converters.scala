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

package ch.datascience.knowledgegraph.projects.rest

import cats.syntax.all._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.testentities._
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder._
import io.renku.jsonld.syntax._

private object Converters extends Converters

private trait Converters {

  implicit lazy val entitiesToKGProject: Project => KGProject = {
    case project: ProjectWithParent =>
      KGProject(
        project.path,
        project.name,
        ProjectCreation(project.dateCreated,
                        project.maybeCreator.map(person => ProjectCreator(person.maybeEmail, person.name))
        ),
        project.visibility,
        project.parent.to[Parent].some,
        project.version
      )
    case project: Project =>
      KGProject(
        project.path,
        project.name,
        ProjectCreation(project.dateCreated,
                        project.maybeCreator.map(person => ProjectCreator(person.maybeEmail, person.name))
        ),
        project.visibility,
        maybeParent = None,
        project.version
      )
  }

  implicit lazy val entitiesToParent: Project => Parent = project =>
    Parent(
      projects.ResourceId(project.asEntityId),
      project.name,
      ProjectCreation(project.dateCreated,
                      project.maybeCreator.map(person => ProjectCreator(person.maybeEmail, person.name))
      )
    )
}
